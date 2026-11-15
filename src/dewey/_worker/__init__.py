"""Internal native-worker building blocks; no supported runner API yet.

These primitives do not claim tasks, persist outcomes or recover leases. A
production runner must supply those guarantees before exposing an entrypoint.
"""
