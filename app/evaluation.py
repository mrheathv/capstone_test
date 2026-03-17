"""
Evaluation framework for the Sales Chatbot.
Provides test case management, scoring, and results display.
"""
import json
import re
import time
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).parent))

from agent.text_to_sql import generate_sql_with_retry, validate_sql
from database.connection import db_query

# ── Constants ────────────────────────────────────────────────────────────────

CONV_PASS_THRESHOLD = 0.6   # weighted score >= this → pass


# ── Data Layer ────────────────────────────────────────────────────────────────

def extract_perf_json(raw_str: str) -> dict | None:
    """Parse the performance metrics JSON blob from a Shahzad Work sheet cell."""
    raw = str(raw_str).replace("=== text_to_sql: Performance Metrics (full) ===\n", "").strip()
    # Fix ,M12 artifact (Excel named-range remnant): replace ,M\d+ with ','
    raw = re.sub(r",M\d+", ",", raw)
    # Remove trailing comma before closing brace/bracket (would be invalid JSON)
    raw = re.sub(r",\s*\n(\s*[}\]])", r"\n\1", raw)
    start = raw.find("{")
    if start == -1:
        return None
    depth = 0
    end = -1
    for idx in range(start, len(raw)):
        if raw[idx] == "{":
            depth += 1
        elif raw[idx] == "}":
            depth -= 1
            if depth == 0:
                end = idx + 1
                break
    if end == -1:
        return None
    try:
        return json.loads(raw[start:end])
    except Exception:
        return None


def seed_from_excel(xlsx_path: str) -> dict:
    """Seed test cases from the Capstone Question Set Excel file."""
    cases: dict = {"sql_output_tests": [], "sql_perf_tests": [], "conversational_tests": []}

    try:
        # --- SQL output tests (sheet: SQL) ---
        sql_df = pd.read_excel(xlsx_path, sheet_name="SQL")
        sql_df.columns = sql_df.columns.str.strip().str.lower()
        i = 1
        for _, row in sql_df.iterrows():
            question = row.get("question")
            sql_query = row.get("sql_query")
            expected = row.get("expected_output", "")
            if not isinstance(question, str) or not question.strip():
                continue
            golden_sql = str(sql_query).strip() if isinstance(sql_query, str) else ""
            # Strip markdown fences if present
            if golden_sql.startswith("```"):
                golden_sql = "\n".join(golden_sql.split("\n")[1:])
            if golden_sql.endswith("```"):
                golden_sql = "\n".join(golden_sql.split("\n")[:-1])
            golden_sql = golden_sql.strip()
            cases["sql_output_tests"].append({
                "id": f"sql_{i:03d}",
                "question": question.strip(),
                "golden_sql": golden_sql,
                "notes": str(expected).strip() if isinstance(expected, str) else "",
            })
            i += 1
    except Exception as e:
        st.warning(f"Could not load SQL sheet from Excel: {e}")

    try:
        # --- Conversational tests (sheet: LLM-Wan, header on row 1) ---
        llm_df = pd.read_excel(xlsx_path, sheet_name="LLM-Wan", header=1)
        llm_df.columns = llm_df.columns.str.strip().str.lower()
        seen = set()
        conv_idx = 1
        for _, row in llm_df.iterrows():
            question = row.get("question")
            purpose = row.get("evaluation purpose", "")
            if not isinstance(question, str) or not question.strip():
                continue
            if question.strip().lower() == "question":
                continue  # skip duplicate header row
            if question.strip() in seen:
                continue
            seen.add(question.strip())
            expected_themes = str(purpose).strip() if isinstance(purpose, str) else ""
            cases["conversational_tests"].append({
                "id": f"conv_{conv_idx:03d}",
                "question": question.strip(),
                "expected_themes": expected_themes,
                "notes": "",
            })
            conv_idx += 1
    except Exception as e:
        st.warning(f"Could not load LLM-Wan sheet from Excel: {e}")

    try:
        # --- SQL performance tests (sheet: Shahzad Work) ---
        # Alternating rows: odd = question row (Q.no + question text)
        #                   even = metrics JSON blob in the Question column
        perf_df = pd.read_excel(xlsx_path, sheet_name="Shahzad Work")
        perf_idx = 1
        rows = list(perf_df.itertuples(index=True))
        i = 0
        while i < len(rows) - 1:
            q_row = rows[i]
            m_row = rows[i + 1]
            i += 2
            q_no = q_row[1]   # Q.no column
            question = str(q_row[2]).strip()   # Question column
            raw_metrics = str(m_row[2])        # metrics JSON in Question column

            if not question or question.lower() == "nan":
                continue

            data = extract_perf_json(raw_metrics)
            if data is None:
                continue

            g = data.get("generation", {})
            e = data.get("execution", {})
            attempts = g.get("attempts", [{}])
            baseline_total_ms = round(g.get("total_ms", 0), 1)
            baseline_llm_ms = round(attempts[0].get("llm_latency_ms", 0), 1) if attempts else 0
            baseline_db_ms = round(attempts[0].get("execution_ms", 0), 2) if attempts else 0
            rows_returned = e.get("rows_returned", 0)
            final_sql = g.get("final_sql", "").strip()

            cases["sql_perf_tests"].append({
                "id": f"perf_{perf_idx:03d}",
                "question": question,
                "golden_sql": final_sql,
                "max_ms_threshold": round(baseline_total_ms * 2),
                "expected_row_count": rows_returned,
                "baseline_total_ms": baseline_total_ms,
                "baseline_llm_ms": baseline_llm_ms,
                "baseline_db_ms": baseline_db_ms,
                "notes": "",
            })
            perf_idx += 1
    except Exception as e:
        st.warning(f"Could not load Shahzad Work sheet from Excel: {e}")

    return cases


def load_test_cases(json_path: str, xlsx_path: str) -> dict:
    """Load test cases from JSON; seed from Excel if file doesn't exist."""
    path = Path(json_path)
    if path.exists():
        with open(path) as f:
            return json.load(f)
    # First run: seed from Excel
    cases = seed_from_excel(xlsx_path)
    save_test_cases(cases, json_path)
    return cases


def save_test_cases(cases: dict, json_path: str) -> None:
    """Persist test cases to JSON."""
    with open(json_path, "w") as f:
        json.dump(cases, f, indent=2)


def generate_id(prefix: str, existing_ids: list) -> str:
    """Generate the next available ID like 'sql_026'."""
    nums = []
    for eid in existing_ids:
        if eid.startswith(prefix + "_"):
            try:
                nums.append(int(eid.split("_")[-1]))
            except ValueError:
                pass
    next_num = max(nums) + 1 if nums else 1
    return f"{prefix}_{next_num:03d}"


# ── Scoring ───────────────────────────────────────────────────────────────────

def _check_mark(val: bool) -> str:
    return "✓" if val else "✗"


def score_sql_output_test(test: dict) -> dict:
    """Run a SQL output test: generate SQL, compare result against golden SQL."""
    result = {
        "id": test["id"],
        "question": test["question"],
        "generated_sql": "",
        "validity": False,
        "executed": False,
        "accuracy": False,
        "accuracy_detail": "",
        "error": "",
        "passed": False,
    }

    try:
        generated_sql, gen_error = generate_sql_with_retry(test["question"], max_attempts=2)
        result["generated_sql"] = generated_sql

        if gen_error:
            result["error"] = f"SQL generation failed: {gen_error}"
            return result

        is_valid, err_msg = validate_sql(generated_sql)
        result["validity"] = is_valid
        if not is_valid:
            result["error"] = err_msg
            return result

        try:
            result_df = db_query(generated_sql)
            result["executed"] = True
        except Exception as e:
            result["error"] = f"Execution error: {e}"
            return result

        # Compare against golden SQL
        try:
            golden_df = db_query(test["golden_sql"])
        except Exception as e:
            result["accuracy"] = False
            result["accuracy_detail"] = f"Golden SQL failed: {e}"
            result["passed"] = False
            return result

        # Sort both by all columns, reset index, compare
        try:
            cols = sorted(golden_df.columns.tolist())
            g = golden_df[cols].sort_values(by=cols).reset_index(drop=True)
            r = result_df[[c for c in cols if c in result_df.columns]]
            r = r.sort_values(by=[c for c in cols if c in r.columns]).reset_index(drop=True)
            match = g.equals(r)
            result["accuracy"] = match
            if match:
                result["accuracy_detail"] = f"{len(golden_df)} rows match"
            else:
                result["accuracy_detail"] = (
                    f"shape mismatch: got {len(result_df)} rows, expected {len(golden_df)} rows"
                    if len(result_df) != len(golden_df)
                    else "row count matches but values differ"
                )
        except Exception as e:
            result["accuracy"] = False
            result["accuracy_detail"] = f"Comparison error: {e}"

    except Exception as e:
        result["error"] = str(e)

    result["passed"] = result["validity"] and result["executed"] and result["accuracy"]
    return result


def score_sql_perf_test(test: dict) -> dict:
    """Run a SQL performance test: measure generation + DB execution time and row count."""
    result = {
        "id": test["id"],
        "question": test["question"],
        "generated_sql": "",
        "validity": False,
        "executed": False,
        "elapsed_total_ms": 0.0,
        "elapsed_db_ms": 0.0,
        "max_ms_threshold": test.get("max_ms_threshold") or 0,
        "baseline_total_ms": test.get("baseline_total_ms") or 0,
        "baseline_db_ms": test.get("baseline_db_ms") or 0,
        "actual_row_count": 0,
        "expected_row_count": test.get("expected_row_count") or 0,
        "time_ok": True,
        "rows_ok": True,
        "error": "",
        "passed": False,
    }

    try:
        # Measure full generation time (includes LLM call)
        t_gen_start = time.perf_counter()
        generated_sql, gen_error = generate_sql_with_retry(test["question"], max_attempts=2)
        elapsed_gen_ms = (time.perf_counter() - t_gen_start) * 1000
        result["generated_sql"] = generated_sql

        if gen_error:
            result["error"] = f"SQL generation failed: {gen_error}"
            return result

        is_valid, err_msg = validate_sql(generated_sql)
        result["validity"] = is_valid
        if not is_valid:
            result["error"] = err_msg
            return result

        # Measure DB execution time separately
        t_db_start = time.perf_counter()
        try:
            result_df = db_query(generated_sql)
            elapsed_db_ms = (time.perf_counter() - t_db_start) * 1000
            result["executed"] = True
            result["elapsed_db_ms"] = round(elapsed_db_ms, 2)
            result["elapsed_total_ms"] = round(elapsed_gen_ms + elapsed_db_ms, 1)
            result["actual_row_count"] = len(result_df)
        except Exception as e:
            result["error"] = f"Execution error: {e}"
            return result

        threshold = result["max_ms_threshold"]
        if threshold and threshold > 0:
            result["time_ok"] = result["elapsed_total_ms"] <= threshold

        expected_rows = result["expected_row_count"]
        if expected_rows and expected_rows > 0:
            result["rows_ok"] = result["actual_row_count"] == expected_rows

    except Exception as e:
        result["error"] = str(e)

    result["passed"] = (
        result["validity"]
        and result["executed"]
        and result["time_ok"]
        and result["rows_ok"]
    )
    return result


def llm_judge(question: str, response: str, expected_themes: str) -> dict:
    """Use GPT to score a conversational response. Returns scores 0-10."""
    from openai import OpenAI
    client = OpenAI()

    prompt = f"""You are an evaluation judge for a sales data chatbot. Score the response below on four dimensions (integers 0-10 each):
- relevance: Does it directly answer what was asked?
- accuracy: Is the information factually correct and data-grounded?
- safety: Is it free of harmful, biased, or inappropriate content?
- other: Overall quality, clarity, and helpfulness.

QUESTION: {question}

EXPECTED THEMES (use as grading rubric): {expected_themes if expected_themes else "No specific themes specified — use general quality criteria."}

CHATBOT RESPONSE:
{response}

Respond ONLY with valid JSON in this exact format (no other text):
{{"relevance": 8, "accuracy": 7, "safety": 10, "other": 8, "rationale": "One concise sentence explaining the scores."}}"""

    try:
        api_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        raw = api_response.choices[0].message.content.strip()
        # Strip markdown fences if model adds them
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:])
        if raw.endswith("```"):
            raw = "\n".join(raw.split("\n")[:-1])
        scores = json.loads(raw.strip())
        return {
            "relevance": int(scores.get("relevance", 0)),
            "accuracy": int(scores.get("accuracy", 0)),
            "safety": int(scores.get("safety", 0)),
            "other": int(scores.get("other", 0)),
            "rationale": str(scores.get("rationale", "")),
        }
    except Exception as e:
        return {"relevance": 0, "accuracy": 0, "safety": 0, "other": 0, "rationale": f"Parse error: {e}"}


def score_conversational_test(test: dict) -> dict:
    """Run a conversational test through the agent and LLM judge."""
    from agent.core import agent_answer

    result = {
        "id": test["id"],
        "question": test["question"],
        "agent_response": "",
        "relevance": 0.0,
        "accuracy": 0.0,
        "safety": 0.0,
        "other": 0.0,
        "weighted_score": 0.0,
        "rationale": "",
        "passed": False,
    }

    # Ensure current_user is set (required by agent_answer)
    st.session_state.setdefault("current_user", "Eval Run")

    try:
        agent_response = agent_answer(test["question"])
        result["agent_response"] = agent_response

        raw = llm_judge(test["question"], agent_response, test.get("expected_themes", ""))
        r = raw["relevance"] / 10.0
        a = raw["accuracy"] / 10.0
        s = raw["safety"] / 10.0
        o = raw["other"] / 10.0
        weighted = round(0.3 * r + 0.3 * a + 0.3 * s + 0.1 * o, 3)

        result["relevance"] = round(r, 2)
        result["accuracy"] = round(a, 2)
        result["safety"] = round(s, 2)
        result["other"] = round(o, 2)
        result["weighted_score"] = weighted
        result["rationale"] = raw["rationale"]
        result["passed"] = weighted >= CONV_PASS_THRESHOLD

    except Exception as e:
        result["rationale"] = f"Error: {e}"

    return result


def run_all_tests(cases: dict) -> dict:
    """Run all test cases and return aggregated results."""
    sql_output_results = []
    sql_perf_results = []
    conv_results = []

    for test in cases.get("sql_output_tests", []):
        sql_output_results.append(score_sql_output_test(test))

    for test in cases.get("sql_perf_tests", []):
        sql_perf_results.append(score_sql_perf_test(test))

    for test in cases.get("conversational_tests", []):
        conv_results.append(score_conversational_test(test))

    output_passed = sum(1 for r in sql_output_results if r["passed"])
    perf_passed = sum(1 for r in sql_perf_results if r["passed"])
    conv_scores = [r["weighted_score"] for r in conv_results]
    conv_passed = sum(1 for r in conv_results if r["passed"])

    summary = {
        "output_total": len(sql_output_results),
        "output_passed": output_passed,
        "output_pass_rate": round(output_passed / len(sql_output_results), 3) if sql_output_results else 0.0,
        "perf_total": len(sql_perf_results),
        "perf_passed": perf_passed,
        "perf_pass_rate": round(perf_passed / len(sql_perf_results), 3) if sql_perf_results else 0.0,
        "conv_total": len(conv_results),
        "conv_passed": conv_passed,
        "conv_avg_score": round(sum(conv_scores) / len(conv_scores), 3) if conv_scores else 0.0,
        "conv_pass_rate": round(conv_passed / len(conv_results), 3) if conv_results else 0.0,
    }

    return {
        "sql_output_results": sql_output_results,
        "sql_perf_results": sql_perf_results,
        "conv_results": conv_results,
        "summary": summary,
    }


# ── UI Helpers ────────────────────────────────────────────────────────────────

def _results_to_df_output(results: list) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "ID": r["id"],
            "Question": r["question"][:80] + "..." if len(r["question"]) > 80 else r["question"],
            "Generated SQL": (r["generated_sql"][:60] + "...") if len(r.get("generated_sql", "")) > 60 else r.get("generated_sql", ""),
            "Valid": _check_mark(r["validity"]),
            "Executed": _check_mark(r["executed"]),
            "Accurate": _check_mark(r["accuracy"]),
            "Detail": r.get("accuracy_detail", ""),
            "Pass": _check_mark(r["passed"]),
            "Error": r.get("error", ""),
        })
    return pd.DataFrame(rows)


def _results_to_df_perf(results: list) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "ID": r["id"],
            "Question": r["question"][:80] + "..." if len(r["question"]) > 80 else r["question"],
            "Valid": _check_mark(r["validity"]),
            "Executed": _check_mark(r["executed"]),
            "Total ms": r.get("elapsed_total_ms", 0),
            "Baseline ms": r.get("baseline_total_ms") or "—",
            "Threshold ms": r.get("max_ms_threshold") or "—",
            "Time OK": _check_mark(r.get("time_ok", True)),
            "DB ms": r.get("elapsed_db_ms", 0),
            "Baseline DB ms": r.get("baseline_db_ms") or "—",
            "Rows": r.get("actual_row_count", 0),
            "Exp Rows": r.get("expected_row_count") or "—",
            "Rows OK": _check_mark(r.get("rows_ok", True)),
            "Pass": _check_mark(r["passed"]),
            "Error": r.get("error", ""),
        })
    return pd.DataFrame(rows)


def _results_to_df_conv(results: list) -> pd.DataFrame:
    rows = []
    for r in results:
        rows.append({
            "ID": r["id"],
            "Question": r["question"][:80] + "..." if len(r["question"]) > 80 else r["question"],
            "Relevance": r["relevance"],
            "Accuracy": r["accuracy"],
            "Safety": r["safety"],
            "Other": r["other"],
            "Weighted Score": r["weighted_score"],
            "Pass": _check_mark(r["passed"]),
            "Rationale": r.get("rationale", ""),
        })
    return pd.DataFrame(rows)


def _render_crud(category: str, cases: dict, json_path: str) -> None:
    """Render Add/Edit/Delete UI for a test category."""
    tests = cases.get(category, [])
    is_perf = category == "sql_perf_tests"
    is_conv = category == "conversational_tests"

    # Display table
    if tests:
        display_cols = ["id", "question"]
        if is_conv:
            display_cols += ["expected_themes", "notes"]
        elif is_perf:
            display_cols += ["golden_sql", "max_ms_threshold", "expected_row_count", "notes"]
        else:
            display_cols += ["golden_sql", "notes"]

        display_df = pd.DataFrame([{k: t.get(k, "") for k in display_cols} for t in tests])
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No test cases yet. Add one below.")

    # Determine edit target
    edit_key = f"eval_edit_{category}"
    if edit_key not in st.session_state:
        st.session_state[edit_key] = None

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if st.button("+ Add", key=f"add_{category}"):
            st.session_state[edit_key] = {"mode": "add", "id": None}

    with col2:
        test_ids = [t["id"] for t in tests]
        if test_ids:
            selected_id = st.selectbox("Select ID to edit/delete", test_ids, key=f"sel_{category}")
        else:
            selected_id = None
            st.selectbox("Select ID to edit/delete", [], key=f"sel_{category}")

    with col3:
        if selected_id and st.button("✏ Edit", key=f"edit_{category}"):
            st.session_state[edit_key] = {"mode": "edit", "id": selected_id}
        if selected_id and st.button("🗑 Delete", key=f"del_{category}"):
            cases[category] = [t for t in tests if t["id"] != selected_id]
            save_test_cases(cases, json_path)
            st.session_state["eval_cases"] = cases
            st.success(f"Deleted {selected_id}")
            st.rerun()

    # Edit/Add form
    target = st.session_state.get(edit_key)
    if target:
        mode = target["mode"]
        existing = next((t for t in tests if t["id"] == target["id"]), {}) if mode == "edit" else {}

        with st.form(key=f"form_{category}_{mode}"):
            st.subheader("Add Test Case" if mode == "add" else f"Edit {target['id']}")
            question = st.text_area("Question *", value=existing.get("question", ""))

            if is_conv:
                expected_themes = st.text_area("Expected Themes", value=existing.get("expected_themes", ""))
                notes = st.text_input("Notes", value=existing.get("notes", ""))
            elif is_perf:
                golden_sql = st.text_area("Golden SQL *", value=existing.get("golden_sql", ""))
                max_ms = st.number_input("Max Time (ms threshold, 0 = skip)", min_value=0, value=int(existing.get("max_ms_threshold") or 0))
                exp_rows = st.number_input("Expected Row Count (0 = skip)", min_value=0, value=int(existing.get("expected_row_count") or 0))
                notes = st.text_input("Notes", value=existing.get("notes", ""))
            else:
                golden_sql = st.text_area("Golden SQL *", value=existing.get("golden_sql", ""))
                notes = st.text_input("Notes", value=existing.get("notes", ""))

            submitted = st.form_submit_button("Save")
            cancelled = st.form_submit_button("Cancel")

            if submitted:
                if not question.strip():
                    st.error("Question is required.")
                else:
                    if mode == "add":
                        new_id = generate_id(
                            "conv" if is_conv else ("perf" if is_perf else "sql"),
                            [t["id"] for t in cases.get(category, [])]
                        )
                        entry: dict[str, Any] = {"id": new_id, "question": question.strip(), "notes": notes}
                        if is_conv:
                            entry["expected_themes"] = expected_themes.strip()
                        elif is_perf:
                            entry["golden_sql"] = golden_sql.strip()
                            entry["max_ms_threshold"] = max_ms if max_ms > 0 else None
                            entry["expected_row_count"] = exp_rows if exp_rows > 0 else None
                        else:
                            entry["golden_sql"] = golden_sql.strip()
                        cases[category].append(entry)
                    else:
                        for t in cases[category]:
                            if t["id"] == target["id"]:
                                t["question"] = question.strip()
                                t["notes"] = notes
                                if is_conv:
                                    t["expected_themes"] = expected_themes.strip()
                                elif is_perf:
                                    t["golden_sql"] = golden_sql.strip()
                                    t["max_ms_threshold"] = max_ms if max_ms > 0 else None
                                    t["expected_row_count"] = exp_rows if exp_rows > 0 else None
                                else:
                                    t["golden_sql"] = golden_sql.strip()

                    save_test_cases(cases, json_path)
                    st.session_state["eval_cases"] = cases
                    st.session_state[edit_key] = None
                    st.success("Saved.")
                    st.rerun()

            if cancelled:
                st.session_state[edit_key] = None
                st.rerun()


# ── Main UI Entry Point ───────────────────────────────────────────────────────

def render_evaluation_tab(json_path: str, xlsx_path: str) -> None:
    """Render the full Evaluation tab."""
    st.header("Evaluation Framework")

    # Load test cases (cached in session state)
    if "eval_cases" not in st.session_state:
        with st.spinner("Loading test cases..."):
            st.session_state["eval_cases"] = load_test_cases(json_path, xlsx_path)
    cases = st.session_state["eval_cases"]

    # ── Run All Tests button ──────────────────────────────────────────────────
    col_run, col_status = st.columns([1, 3])
    with col_run:
        run_disabled = st.session_state.get("eval_running", False)
        run_clicked = st.button(
            "▶ Run All Tests",
            disabled=run_disabled,
            type="primary",
            help="Runs all enabled test cases. This may take 1-2 minutes."
        )

    total = (
        len(cases.get("sql_output_tests", []))
        + len(cases.get("sql_perf_tests", []))
        + len(cases.get("conversational_tests", []))
    )
    with col_status:
        st.caption(f"{total} test cases loaded")

    if run_clicked:
        st.session_state["eval_running"] = True
        with st.spinner("Running tests... this may take a minute or two."):
            results = run_all_tests(cases)
        st.session_state["eval_results"] = results
        st.session_state["eval_running"] = False
        st.rerun()

    st.divider()

    # ── Test Case Management tabs ─────────────────────────────────────────────
    st.subheader("Test Cases")
    tab_out, tab_perf, tab_conv = st.tabs([
        f"SQL Output Tests ({len(cases.get('sql_output_tests', []))})",
        f"SQL Performance Tests ({len(cases.get('sql_perf_tests', []))})",
        f"Conversational Tests ({len(cases.get('conversational_tests', []))})",
    ])

    with tab_out:
        st.caption("Pass criteria: generated SQL is valid, executes, and returns the same rows as the golden SQL.")
        _render_crud("sql_output_tests", cases, json_path)

    with tab_perf:
        st.caption("Pass criteria: generated SQL is valid, executes, runs under the time threshold, and returns the expected row count. Leave threshold/count at 0 to skip that check.")
        _render_crud("sql_perf_tests", cases, json_path)

    with tab_conv:
        st.caption(f"Pass criteria: LLM-as-judge weighted score ≥ {CONV_PASS_THRESHOLD} (Relevance×0.3 + Accuracy×0.3 + Safety×0.3 + Other×0.1).")
        _render_crud("conversational_tests", cases, json_path)

    # ── Results Dashboard ─────────────────────────────────────────────────────
    results = st.session_state.get("eval_results")
    if results:
        st.divider()
        st.subheader("Results")

        s = results["summary"]

        # Summary metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        with m1:
            st.metric(
                "SQL Output Pass Rate",
                f"{s['output_passed']}/{s['output_total']}",
                f"{s['output_pass_rate']*100:.0f}%" if s["output_total"] else "N/A",
            )
        with m2:
            st.metric(
                "SQL Perf Pass Rate",
                f"{s['perf_passed']}/{s['perf_total']}",
                f"{s['perf_pass_rate']*100:.0f}%" if s["perf_total"] else "N/A",
            )
        with m3:
            st.metric(
                "Conv Pass Rate",
                f"{s['conv_passed']}/{s['conv_total']}",
                f"{s['conv_pass_rate']*100:.0f}%" if s["conv_total"] else "N/A",
            )
        with m4:
            st.metric("Conv Avg Score", f"{s['conv_avg_score']:.3f}" if s["conv_total"] else "N/A")
        with m5:
            all_total = s["output_total"] + s["perf_total"] + s["conv_total"]
            all_passed = s["output_passed"] + s["perf_passed"] + s["conv_passed"]
            rate = f"{all_passed/all_total*100:.0f}%" if all_total else "N/A"
            st.metric("Overall Pass Rate", f"{all_passed}/{all_total}", rate)

        # Per-test result tables
        res_tab1, res_tab2, res_tab3 = st.tabs(["SQL Output Results", "SQL Perf Results", "Conv Results"])

        with res_tab1:
            if results["sql_output_results"]:
                df = _results_to_df_output(results["sql_output_results"])
                st.dataframe(df, use_container_width=True)
                csv = df.to_csv(index=False)
                st.download_button("⬇ Download CSV", csv, "sql_output_results.csv", "text/csv")
            else:
                st.info("No SQL output test results.")

        with res_tab2:
            if results["sql_perf_results"]:
                df = _results_to_df_perf(results["sql_perf_results"])
                st.dataframe(df, use_container_width=True)
                csv = df.to_csv(index=False)
                st.download_button("⬇ Download CSV", csv, "sql_perf_results.csv", "text/csv")
            else:
                st.info("No SQL performance test results.")

        with res_tab3:
            if results["conv_results"]:
                df = _results_to_df_conv(results["conv_results"])
                st.dataframe(df, use_container_width=True)
                csv = df.to_csv(index=False)
                st.download_button("⬇ Download CSV", csv, "conv_results.csv", "text/csv")
            else:
                st.info("No conversational test results.")
