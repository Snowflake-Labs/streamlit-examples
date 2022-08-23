"""
Copyright (c) 2022 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Iterable

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import Table
from toolz.itertoolz import pluck

from lib.chart_helpers import mk_labels, mk_links
from lib.filterwidget import MyFilter

MY_TABLE = "CUSTOMERS"


def _get_active_filters() -> filter:
    return filter(lambda _: _.is_enabled, st.session_state.filters)


def _is_any_filter_enabled() -> bool:
    return any(pluck("is_enabled", st.session_state.filters))


def _get_human_filter_names(_iter: Iterable) -> Iterable:
    return pluck("human_name", _iter)


# Initialize connection.
def init_connection() -> Session:
    return Session.builder.configs(st.secrets["snowpark"]).create()


@st.cache
def convert_df(df: pd.DataFrame):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def draw_sidebar():
    """Should include dynamically generated filters"""

    with st.sidebar:
        selected_filters = st.multiselect(
            "Select which filters to enable",
            list(_get_human_filter_names(st.session_state.filters)),
            [],
        )
        for _f in st.session_state.filters:
            if _f.human_name in selected_filters:
                _f.enable()

        if _is_any_filter_enabled():
            with st.form(key="input_form"):

                for _f in _get_active_filters():
                    _f.create_widget()
                st.session_state.clicked = st.form_submit_button(label="Submit")
        else:
            st.write("Please enable a filter")


def draw_main_ui(_session: Session):
    """Contains the logic and the presentation of main section of UI"""
    if _is_any_filter_enabled():

        customers: Table = _session.table(MY_TABLE)
        table_sequence = [customers]

        _f: MyFilter
        for _f in _get_active_filters():
            # This block generates the sequence of dataframes as continually applying AND filter set by the sidebar
            # The dataframes are to be used in the Sankey chart.

            # First, get the last dataframe in the list
            last_table = table_sequence[-1]
            # Apply the current filter to it
            new_table = last_table[
                # At this point the filter will be dynamically applied to the dataframe using the API from MyFilter
                _f(last_table)
            ]
            table_sequence += [new_table]
        st.header("Dataframe preview")

        st.write(table_sequence[-1].sample(n=5).to_pandas().head())

        # Generate the Sankey chart
        fig = go.Figure(
            data=[
                go.Sankey(
                    node=dict(
                        pad=15,
                        thickness=20,
                        line=dict(color="black", width=0.5),
                        label=mk_labels(_get_human_filter_names(_get_active_filters())),
                    ),
                    link=mk_links(table_sequence),
                )
            ]
        )
        st.header("Sankey chart of the applied filters")
        st.plotly_chart(fig, use_container_width=True)

        # Add the SQL statement sequence table
        statement_sequence = """
| number | filter name | query, transformation |
| ------ | ----------- | --------------------- |"""
        st.header("Statement sequence")
        for number, (_label, _table) in enumerate(
            zip(
                mk_labels(_get_human_filter_names(_get_active_filters())),
                table_sequence,
            )
        ):
            statement_sequence += f"""\n| {number+1} | {_label} | ```{_table.queries['queries'][0]}``` |"""

        st.markdown(statement_sequence)

        # Materialize the result <=> the button was clicked
        if st.session_state.clicked:
            with st.spinner("Converting results..."):
                st.download_button(
                    label="Download as CSV",
                    data=convert_df(table_sequence[-1].to_pandas()),
                    file_name="customers.csv",
                    mime="text/csv",
                )
    else:
        st.write("Please enable a filter in the sidebar to show transformations")


if __name__ == "__main__":
    # Initialize the filters
    session = init_connection()
    MyFilter.session = session
    MyFilter.table_name = MY_TABLE

    st.session_state.filters = (
        MyFilter(
            human_name="Current customer",
            table_column="is_current_customer",
            widget_id="current_customer",
            widget_type=st.checkbox,
        ),
        MyFilter(
            human_name="Tenure",
            table_column="years_tenure",
            widget_id="tenure_slider",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="Weekly workout count",
            table_column="average_weekly_workout_count",
            widget_id="workouts_slider",
            widget_type=st.select_slider,
        ),
    )

    draw_sidebar()
    draw_main_ui(session)
