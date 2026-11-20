"""Snowflake DB Clone Tool

Clones a database for development and sets up who can use the clone: the
owner role owns it and everything in it, the read-only role can only read
it. The app runs its queries as its owner role, CLONE_TOOL, whoever opens
it (setup.sql has what that role is allowed to do)
"""

import re

import streamlit as st

TOOL_ROLE = "CLONE_TOOL"
SYSTEM_ROLES = {"ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN", "PUBLIC", TOOL_ROLE}
SYSTEM_DATABASES = {"SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"}
NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,254}")

st.set_page_config(page_title="Snowflake DB Clone Tool", page_icon="🧩")
session = st.connection("snowflake").session()


def query(sql):
    return [
        {key.strip('"').lower(): value for key, value in row.as_dict().items()}
        for row in session.sql(sql).collect()
    ]


@st.cache_data(ttl=600)
def load_roles():
    try:
        roles = query("SHOW ROLES")
    except Exception:
        return [], None, []
    names = sorted(role["name"] for role in roles)
    current = next((role["name"] for role in roles if role.get("is_current") == "Y"), None)
    inherited = sorted(role["name"] for role in roles if role.get("is_inherited") == "Y")
    return names, current, inherited


def find_database(name):
    for row in query(f"SHOW DATABASES LIKE '{name}'"):
        if row["name"] == name:
            return row
    return None


def check(target, source, owner, reader, roles):
    fields = {
        "Target DB name": target,
        "Clone from DB": source,
        "Owner role": owner,
        "Read-only role": reader,
    }
    problems = [
        f"{label}: use letters, digits and _, starting with a letter or _."
        for label, value in fields.items()
        if not NAME.fullmatch(value)
    ]
    if problems:
        return problems, None

    if target == source:
        problems.append("The target is the database it would be cloned from.")
    for name in sorted({target, source} & SYSTEM_DATABASES):
        problems.append(f"{name} is a Snowflake system database.")
    if owner == reader:
        problems.append("The owner role and the read-only role are the same role.")
    for role in (owner, reader):
        if roles and role not in roles:
            problems.append(f"Role {role} doesn't exist.")
    if find_database(source) is None:
        problems.append(f"Database {source} doesn't exist, or the app's role can't see it.")

    existing = find_database(target)
    if existing and existing["owner"] != owner:
        problems.append(
            f"{target} already exists and belongs to {existing['owner'] or 'no role'}, not {owner}. "
            "The tool only recreates databases that already belong to the owner role."
        )
    return problems, existing


def clone_sql(target, source, owner, reader):
    return f"""\
DROP DATABASE IF EXISTS {target};
CREATE DATABASE {target} CLONE {source};

-- {owner} owns the clone and everything in it. The new database belongs to
-- the role that created it, and the cloned objects keep their owners from {source}
GRANT OWNERSHIP ON DATABASE {target} TO ROLE {owner} REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE {target} TO ROLE {owner} REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN DATABASE {target} TO ROLE {owner} REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN DATABASE {target} TO ROLE {owner} REVOKE CURRENT GRANTS;

-- {reader} can open the database and its schemas and read the tables and views
GRANT USAGE ON DATABASE {target} TO ROLE {reader};
GRANT USAGE ON ALL SCHEMAS IN DATABASE {target} TO ROLE {reader};
GRANT SELECT ON ALL TABLES IN DATABASE {target} TO ROLE {reader};
GRANT SELECT ON ALL VIEWS IN DATABASE {target} TO ROLE {reader};

-- and the same for schemas, tables and views created in it later
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {target} TO ROLE {reader};
GRANT SELECT ON FUTURE TABLES IN DATABASE {target} TO ROLE {reader};
GRANT SELECT ON FUTURE VIEWS IN DATABASE {target} TO ROLE {reader};
"""


def statements(sql):
    code = "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
    return [part.strip() for part in code.split(";") if part.strip()]


def run(sql):
    todo = statements(sql)
    with st.status(f"Running {len(todo)} statements...", expanded=True) as status:
        for statement in todo:
            try:
                session.sql(statement).collect()
            except Exception as error:
                st.error(f"`{statement}`\n\n{getattr(error, 'message', None) or error}")
                status.update(label="Stopped at an error, the statements after it didn't run", state="error")
                return False
            st.markdown(f"✅ `{statement}`")
        status.update(label=f"Done: {len(todo)} statements", state="complete", expanded=False)
    return True


def grants(database):
    rows = [(row, "") for row in query(f"SHOW GRANTS ON DATABASE {database}")]
    rows += [(row, "FUTURE ") for row in query(f"SHOW FUTURE GRANTS IN DATABASE {database}")]
    return [
        {
            "privilege": row.get("privilege"),
            "on": prefix + str(row.get("granted_on") or row.get("grant_on")),
            "name": row.get("name"),
            "role": row.get("grantee_name"),
        }
        for row, prefix in rows
    ]


st.title("🧩 Snowflake DB Clone Tool")

roles, app_role, inherited = load_roles()
if app_role:
    st.caption(f"The app runs its queries as {app_role}.")

target = st.text_input(
    "Target DB name",
    "YOUR_DB_NAME_WITH_YOUR_NAME",
    help="A new database, or one of yours to clone again. Put your name in it.",
).strip().upper()
source = st.text_input("Clone from DB", "DEV").strip().upper()

owners = [role for role in (inherited or roles) if role not in SYSTEM_ROLES]
if owners:
    owner = st.selectbox(
        "Owner role",
        owners,
        index=owners.index("DEVELOPMENT") if "DEVELOPMENT" in owners else 0,
        help=f"Roles the app can act for. To add one: GRANT ROLE <role> TO ROLE {TOOL_ROLE}",
    )
else:
    owner = st.text_input("Owner role", "DEVELOPMENT").strip().upper()
reader = st.text_input("Read-only role", "READ_ONLY_ROLE").strip().upper()

problems, existing = check(target, source, owner, reader, roles)
for problem in problems:
    st.error(problem)
if existing and not problems:
    st.warning(
        f"{target} already exists (owner: {owner}). It will be dropped and recreated if you proceed.",
        icon="⚠️",
    )

if st.button("🔄 Refresh SQL"):
    st.cache_data.clear()
    st.rerun()

st.subheader("Generated SQL")
st.info("Please, always check db names you set up!")
if problems:
    st.caption("Fix the problems above to get the SQL.")
    st.stop()

sql = clone_sql(target, source, owner, reader)
st.code(sql, language="sql")

confirmed = True
if existing:
    confirmed = st.checkbox(f"Drop {target} and clone it again", key=f"confirm_{target}")

if st.button("🚀 Execute SQL", type="primary", disabled=not confirmed):
    if run(sql):
        st.success(f"{target} is ready: cloned from {source}, owned by {owner}, readable by {reader}.")
        try:
            st.dataframe(grants(target), hide_index=True)
        except Exception as error:
            st.caption(f"Couldn't read the grants back: {error}")
