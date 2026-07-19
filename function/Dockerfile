FROM python:3.13-slim

WORKDIR /function
COPY requirements.txt .
RUN pip install --no-cache-dir --target /python -r requirements.txt
COPY func.py partition_loader.py work_queue.py .
# OCI Functions runs the FDK without root privileges. Ensure the handler and
# its helper module remain readable after the restricted deployment build.
RUN chmod 755 /function && chmod 644 /function/func.py /function/partition_loader.py /function/work_queue.py
# The FDK loads func.py by absolute path, so add the adjacent /function
# directory explicitly for imports of bundled helper modules.
ENV PYTHONPATH=/python:/function
CMD ["/python/bin/fdk", "/function/func.py", "handler"]
