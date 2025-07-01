from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.runnables import RunnablePassthrough
import pandas as pd
import db
import streamlit as st
from decouple import config

db_pass = st.text_input("DB Password", key="db_pass",type="password")
db.db_password = db_pass  # Set the password for the db module
api_key = st.text_input("API KEY", key="api_key")

st.write("# Text to SQL Query Generator ")

user_query = st.text_input("Enter your query", key="query_input")

st.write("Your query is :", user_query)

# 1. Set your schema as plain text
schema_context = """
### Tables and relationships for the Stock Market DB
SCHEMA REFERENCE – STOCK‑MARKET DATA WAREHOUSE
==============================================

TABLE: instrument   (INSTRUMENT MASTER)
--------------------------------------------------
  Id                   BIGINT  PK  auto‑increment : Surrogate key for every instrument
  Name                 VARCHAR(200)              : Legal or common name of the instrument
  CurrencyId           BIGINT  FK → currency     : Trading / pricing currency
  BloombergTicker      VARCHAR(50)               : Bloomberg symbol
  ReutersRic           VARCHAR(50)               : Refinitiv RIC code
  BloombergID          VARCHAR(50)               : Bloomberg unique identifier (BBGID)
  ExchangeID           BIGINT  FK → exchange     : Home listing exchange
  InstrumentTypeId     BIGINT  FK → instrumenttype : Equity, Bond, Crypto, etc.
  ModifyUserid         INTEGER FK → auth_user    : User who last modified the row
  ModifyDateTime       DATETIME(6)               : Timestamp of last modification
  CountryId            BIGINT  FK → country      : Country of issuance
  DomicileId           BIGINT  FK → country      : Corporate domicile
  IndustryId           BIGINT  FK → industry     : Internal industry classification
  SuperSectorId        BIGINT  FK → supersector  : High‑level sector tag
  SectorId             BIGINT  FK → sector       : Mid‑level sector tag
  SubSectorId          BIGINT  FK → subsector    : Fine‑grained sector tag
  ISIN                 VARCHAR(100)              : International Securities ID
  Ticker               VARCHAR(50)               : Native exchange ticker (no suffix)
  gicsubindustryid     BIGINT  FK → gicsubindustry
  gicindustryid        BIGINT  FK → gicindustry
  gicindustrygroupid   BIGINT  FK → gicindustrygroup
  gicsectorid          BIGINT  FK → gicsector
  activemarketdata     INTEGER                   : 1 = live data feed enabled
  active               INTEGER DEFAULT 0         : 1 = instrument is active
  activeDailyUpdate    TINYINT(1)                : 1 = should be updated every day

TABLE: instrument_vmesg_momentummodel   (MOMENTUM MODEL HEADER)
----------------------------------------------------------------
  id                   INTEGER PK auto‑inc       : Model record id
  name                 VARCHAR(50)              : Optional label for model run
  active               TINYINT(1)               : 1 = current / in use
  date                 DATE                     : Observation / signal date
  ModifyDateTime       DATETIME(6)              : Audit timestamp
  instrument_id        BIGINT  FK → instrument  : Underlying instrument
  ModifyUserid         INTEGER FK → auth_user   : Who created / modified

TABLE: instrument_vmesg_momentumvaluesmodel   (MOMENTUM BUCKETS)
-----------------------------------------------------------------
  id                   INTEGER PK auto‑inc      : Row id
  momentum             DOUBLE                   : Momentum score (positive = up‑trend)
  no_of_days           INTEGER                  : Look‑back window length in days
  momentum_id_id       INTEGER FK → instrument_vmesg_momentummodel.id : Parent header

TABLE: instrument_vmesg_volatilitymodel   (VOLATILITY MODEL)
-------------------------------------------------------------
  id                   INTEGER PK auto‑inc      : Model id
  name                 VARCHAR(50)              : Optional label
  active               TINYINT(1)               : 1 = current
  date                 DATE                     : Observation date
  volatility           DOUBLE                   : Volatility value (e.g., 0.25 = 25 %)
  ModifyDateTime       DATETIME(6)              : Audit timestamp
  instrument_id        BIGINT  FK → instrument  : Underlying instrument
  ModifyUserid         INTEGER FK → auth_user   : Editor

TABLE: instrumentadtv   (AVERAGE DAILY TRADING VOLUME)
-------------------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  InstrumentId         BIGINT  FK → instrument  : Instrument
  CurrencyId           BIGINT  FK → currency    : Currency used for ADTV value
  Date                 DATE                     : Calculation cut‑off date
  ADTV                 DECIMAL(38,18)           : Average daily traded volume
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp

TABLE: instrumentchildbond   (BOND‑LEVEL DETAIL)
------------------------------------------------
  id                   INTEGER PK auto‑inc      : Bond record id
  instrumentid         BIGINT  FK → instrument  : Parent issuer
  date                 DATE                     : Reference / pricing date
  paramount            DECIMAL(38,10)           : Outstanding principal (par)
  coupon               DECIMAL(38,10)           : Stated coupon rate %
  bondyield            DECIMAL(38,10)           : Yield‑to‑maturity on reference date
  maturity             DATE                     : Maturity date of the bond
  rating               VARCHAR(20)              : Credit rating
  issuername           VARCHAR(2000)            : Issuer’s full legal name
  sector               VARCHAR(200)             : Borrower sector
  modifyuserid         INTEGER FK → auth_user   : Editor
  modifydatetime       DATETIME                 : Audit timestamp

TABLE: instrumentesgscore   (ESG SCORES & CARBON DATA)
------------------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  InstrumentId         BIGINT  FK → instrument  : Instrument
  Date                 DATE                     : Snapshot date
  ESG                  DECIMAL(38,18)           : Aggregate ESG score
  Environmental        DECIMAL(38,18)           : Environmental pillar
  Social               DECIMAL(38,18)           : Social pillar
  Governance           DECIMAL(38,18)           : Governance pillar
  Carbonfootprint      DECIMAL(38,18)           : Carbon footprint metric
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp
  source_id            BIGINT  FK → source      : Data vendor / methodology

TABLE: instrumentfreefloat_nos   (FREE‑FLOAT SHARE COUNT)
---------------------------------------------------------
  id                   INTEGER PK auto‑inc      : Row id
  date                 DATE                     : Snapshot date
  nos                  DOUBLE                   : Free‑float shares outstanding
  ModifyDateTime       DATETIME(6)              : Audit timestamp
  active               INTEGER                  : 1 = current
  instrumentid_id      BIGINT  FK → instrument  : Instrument
  ModifyUserid         INTEGER FK → auth_user   : Editor

TABLE: instrumentfreefloatmarketcap   (FREE‑FLOAT MARKET CAP)
-------------------------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  InstrumentId         BIGINT  FK → instrument  : Instrument
  CurrencyId           BIGINT  FK → currency    : Currency
  Date                 DATE                     : Snapshot date
  sourceid             BIGINT  FK → source      : Data vendor
  MarketCap            DECIMAL(38,18)           : Float‑adjusted market cap
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp

TABLE: instrumentmarketcap   (FULL MARKET CAP)
----------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  InstrumentId         BIGINT  FK → instrument  : Instrument
  CurrencyId           BIGINT  FK → currency    : Currency
  Date                 DATE                     : Snapshot date
  MarketCap            DECIMAL(38,18)           : Total market value (shares × price)
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp

TABLE: instrumentprice   (HISTORICAL PRICES)
--------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  InstrumentId         BIGINT  FK → instrument  : Instrument
  Date                 DATE                     : Trade / close date
  Price                DECIMAL(38,10)           : EOD or fix price
  FixingTypeId         BIGINT  FK → fixingtype  : Pricing convention
  SourceId             BIGINT  FK → source      : Data source
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp

TABLE: instrumentprice_live   (REAL‑TIME PRICES)
------------------------------------------------
  id                   BIGINT PK auto‑inc       : Row id
  instrumentid         BIGINT  FK → instrument  : Instrument
  date                 DATE                     : Quote date (usually current)
  price                DECIMAL(38,18)           : Real‑time price
  fixingtypeid         BIGINT  FK → fixingtype  : Price basis (bid, mid, etc.)
  sourceid             BIGINT  FK → source      : Data source
  modifyuserid         INTEGER FK → auth_user   : Writer process / user
  modifydatetime       DATETIME                 : Timestamp of quote ingestion

TABLE: instrumentquantittativefactor   (COMBINED MOMENTUM & VOLATILITY)
------------------------------------------------------------------------
  id                   BIGINT PK auto‑inc       : Row id
  instrumentid         BIGINT  FK → instrument  : Instrument
  factordate           DATE                     : Factor snapshot date
  volatility           DECIMAL(24,12)           : Volatility value
  momentum             DECIMAL(24,12)           : Momentum value
  ModifyUserid         INTEGER FK → auth_user   : Editor
  modifyDateTime       DATETIME(6)              : Audit timestamp

TABLE: instrumenttype   (LOOKUP – INSTRUMENT CATEGORIES)
--------------------------------------------------------
  Id                   BIGINT PK auto‑inc       : Row id
  Name                 VARCHAR(50)              : Category label (Equity, Bond, Crypto)
  ModifyUserid         INTEGER FK → auth_user   : Editor
  ModifyDateTime       DATETIME(6)              : Audit timestamp


RELATIONSHIPS SUMMARY (FOREIGN‑KEY MAP)
========================================
  instrument.CurrencyId                 → currency.Id
  instrument.ExchangeID                 → exchange.Id
  instrument.InstrumentTypeId           → instrumenttype.Id
  instrument.CountryId                  → country.Id
  instrument.DomicileId                 → country.Id
  instrument.IndustryId                 → industry.Id
  instrument.SuperSectorId              → supersector.Id
  instrument.SectorId                   → sector.Id
  instrument.SubSectorId                → subsector.Id
  instrument.gicsubindustryid           → gicsubindustry.Id
  instrument.gicindustryid              → gicindustry.Id
  instrument.gicindustrygroupid         → gicindustrygroup.Id
  instrument.gicsectorid                → gicsector.Id
  instrument.ModifyUserid               → auth_user.Id

  instrument_vmesg_momentummodel.instrument_id            → instrument.Id
  instrument_vmesg_momentummodel.ModifyUserid             → auth_user.Id
  instrument_vmesg_momentumvaluesmodel.momentum_id_id     → instrument_vmesg_momentummodel.id

  instrument_vmesg_volatilitymodel.instrument_id          → instrument.Id
  instrument_vmesg_volatilitymodel.ModifyUserid           → auth_user.Id

  instrumentadtv.InstrumentId           → instrument.Id
  instrumentadtv.CurrencyId             → currency.Id
  instrumentadtv.ModifyUserid           → auth_user.Id

  instrumentchildbond.instrumentid      → instrument.Id
  instrumentchildbond.modifyuserid      → auth_user.Id

  instrumentesgscore.InstrumentId       → instrument.Id
  instrumentesgscore.source_id          → source.Id
  instrumentesgscore.ModifyUserid       → auth_user.Id

  instrumentfreefloat_nos.instrumentid_id → instrument.Id
  instrumentfreefloat_nos.ModifyUserid     → auth_user.Id

  instrumentfreefloatmarketcap.InstrumentId → instrument.Id
  instrumentfreefloatmarketcap.CurrencyId   → currency.Id
  instrumentfreefloatmarketcap.sourceid     → source.Id
  instrumentfreefloatmarketcap.ModifyUserid → auth_user.Id

  instrumentmarketcap.InstrumentId      → instrument.Id
  instrumentmarketcap.CurrencyId        → currency.Id
  instrumentmarketcap.ModifyUserid      → auth_user.Id

  instrumentprice.InstrumentId          → instrument.Id
  instrumentprice.FixingTypeId          → fixingtype.Id
  instrumentprice.SourceId              → source.Id
  instrumentprice.ModifyUserid          → auth_user.Id

  instrumentprice_live.instrumentid     → instrument.Id
  instrumentprice_live.fixingtypeid     → fixingtype.Id
  instrumentprice_live.sourceid         → source.Id
  instrumentprice_live.modifyuserid     → auth_user.Id

  instrumentquantittativefactor.instrumentid → instrument.Id
  instrumentquantittativefactor.ModifyUserid  → auth_user.Id

  instrumenttype.ModifyUserid           → auth_user.Id


"""

# 2. Connect to OpenAI (requires valid API key)
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    openai_api_key=api_key
)
# 3. Create prompt template
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        """You are an expert MySQL SQL generator. Only use the schema provided below to write SQL queries.
Return only the final SQL query using **MySQL-compatible syntax** and nothing else.

Schema:
{schema}
"""
    ),
    ("human", "{question}")
])


# 4. Build the generation chain
sql_chain = (
    {"schema": RunnablePassthrough(), "question": RunnablePassthrough()}
    | prompt
    | llm
)


# 5. Ask a natural language question
question = user_query
response = sql_chain.invoke({
    "schema": schema_context,
    "question": question
})

# 6. Print result
print("\nGenerated SQL Query:\n")
print(response.content)
st.write("Your edited query is :", response.content)

edited_user_query = st.text_area("Generated SQL Query", key="edited_user_query",value=response.content, height=200)


is_click = st.button("Execute SQL Query")
if is_click:
    st.write("Executing SQL Query...")
    query = edited_user_query
    results = db.run_query(query)
    
    if isinstance(results, pd.DataFrame):
        st.write("Query Results:")
        st.dataframe(results)
    else:
        st.write("Query executed successfully, but no results returned.") 