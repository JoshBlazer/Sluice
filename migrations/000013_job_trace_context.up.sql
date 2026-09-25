-- W3C traceparent of the request that submitted the job, so the worker's
-- execution (and every retry) joins the submission's distributed trace.
ALTER TABLE jobs ADD COLUMN trace_context TEXT;
