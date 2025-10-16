import os
import pathlib

import pytest
from arro3.core import Array, DataType, Field, Table

from deltalake import DeltaTable, init_tracing, write_deltalake


@pytest.fixture(scope="function")
def memory_exporter():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    provider = trace.get_tracer_provider()
    if hasattr(provider, "add_span_processor"):
        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

    yield exporter
    exporter.clear()


@pytest.mark.opentelemetry
def test_tracing_simple(memory_exporter):
    from opentelemetry import trace

    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("test-span"):
        pass

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "test-span"


@pytest.mark.opentelemetry
def test_tracing_with_write_operation(memory_exporter, tmp_path: pathlib.Path):
    from opentelemetry import trace

    tracer = trace.get_tracer(__name__)

    sample_data = Table(
        {
            "id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True)),
            "value": Array(
                ["a", "b", "c"], Field("value", type=DataType.string(), nullable=True)
            ),
        }
    )

    table_path = str(tmp_path / "test_table")

    with tracer.start_as_current_span("delta_write_operation") as span:
        span.set_attribute("table.path", table_path)
        span.set_attribute("operation.mode", "overwrite")

        write_deltalake(table_path, sample_data, mode="overwrite")

        dt = DeltaTable(table_path)
        span.set_attribute("table.version", dt.version())

    spans = memory_exporter.get_finished_spans()
    assert len(spans) >= 1
    assert spans[0].name == "delta_write_operation"
    assert spans[0].attributes["table.path"] == table_path
    assert spans[0].attributes["operation.mode"] == "overwrite"
    assert spans[0].attributes["table.version"] == 0
    assert spans[0].status.is_ok


@pytest.mark.opentelemetry
def test_tracing_nested_spans(memory_exporter, tmp_path: pathlib.Path):
    from opentelemetry import trace

    tracer = trace.get_tracer(__name__)

    sample_data = Table(
        {
            "id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True)),
            "value": Array(
                ["a", "b", "c"], Field("value", type=DataType.string(), nullable=True)
            ),
        }
    )

    table_path = str(tmp_path / "test_table")

    with tracer.start_as_current_span("delta_workflow") as parent:
        parent.set_attribute("workflow.type", "etl")

        with tracer.start_as_current_span("write_phase"):
            write_deltalake(table_path, sample_data, mode="overwrite")

        with tracer.start_as_current_span("append_phase"):
            write_deltalake(table_path, sample_data, mode="append")

    spans = memory_exporter.get_finished_spans()
    assert len(spans) >= 3

    parent_spans = [s for s in spans if s.name == "delta_workflow"]
    assert len(parent_spans) > 0
    parent_span = parent_spans[0]

    child_spans = [s for s in spans if s.name != "delta_workflow"]
    for child in child_spans:
        assert child.parent.span_id == parent_span.context.span_id


@pytest.mark.opentelemetry
def test_tracing_with_error_handling(memory_exporter, tmp_path: pathlib.Path):
    from opentelemetry import trace
    from opentelemetry.trace import StatusCode

    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("operation_with_error") as span:
        try:
            invalid_path = "/invalid/path/that/does/not/exist"
            DeltaTable(invalid_path)
        except Exception as e:
            span.set_status(StatusCode.ERROR, str(e))
            span.record_exception(e)

    spans = memory_exporter.get_finished_spans()
    assert len(spans) == 1

    error_span = spans[0]
    assert error_span.name == "operation_with_error"
    assert not error_span.status.is_ok
    assert len(error_span.events) >= 1
    assert error_span.events[0].name == "exception"


@pytest.mark.opentelemetry
def test_tracing_with_attributes_and_events(memory_exporter, tmp_path: pathlib.Path):
    from opentelemetry import trace

    tracer = trace.get_tracer(__name__)

    sample_data = Table(
        {
            "id": Array([1, 2, 3], Field("id", type=DataType.int64(), nullable=True)),
            "value": Array(
                ["a", "b", "c"], Field("value", type=DataType.string(), nullable=True)
            ),
        }
    )

    table_path = str(tmp_path / "test_table")

    with tracer.start_as_current_span("delta_operation") as span:
        span.set_attribute("table.name", "test_table")
        span.add_event("preparing_data")

        write_deltalake(table_path, sample_data, mode="overwrite")
        span.add_event("data_written")

        dt = DeltaTable(table_path)
        span.add_event("operation_complete", {"version": dt.version()})

    spans = memory_exporter.get_finished_spans()
    assert len(spans) >= 1

    span = spans[0]
    assert span.attributes["table.name"] == "test_table"

    event_names = [event.name for event in span.events]
    assert "preparing_data" in event_names
    assert "data_written" in event_names
    assert "operation_complete" in event_names


@pytest.mark.opentelemetry
def test_init_tracing_with_env_vars():
    original_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    original_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS")

    try:
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://env-endpoint:4317"
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = (
            "x-api-key=env-key,authorization=Bearer env-token"
        )

        init_tracing()
        init_tracing()

    finally:
        if original_endpoint is not None:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = original_endpoint
        else:
            os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)

        if original_headers is not None:
            os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = original_headers
        else:
            os.environ.pop("OTEL_EXPORTER_OTLP_HEADERS", None)
