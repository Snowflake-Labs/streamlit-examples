CREATE OR REPLACE DATABASE STREAMLIT_DEMO;
CREATE OR REPLACE SCHEMA STREAMLIT_DEMO.STREAMLIT_USER_PROFILER;
CREATE OR REPLACE SEQUENCE STREAMLIT_DEMO.STREAMLIT_USER_PROFILER.user_id_seq;

CREATE OR REPLACE TABLE STREAMLIT_DEMO.STREAMLIT_USER_PROFILER.CUSTOMERS(
    customer_id INT default user_id_seq.nextval,
    years_tenure INT,
    average_weekly_workout_count INT,
    is_current_customer BOOLEAN,
    customer_email VARCHAR
);

INSERT INTO
    STREAMLIT_DEMO.STREAMLIT_USER_PROFILER.CUSTOMERS (
        years_tenure,
        average_weekly_workout_count,
        is_current_customer,
        customer_email
    )
SELECT
abs(random() % 25), -- years_tenure, from 1998
abs(random() % 8), -- weekly workout count, average
abs(random() % 8), -- is current customer
lower(randstr(5, random())) || '@example.org'
FROM
    table(generator(rowCount => 10000000));