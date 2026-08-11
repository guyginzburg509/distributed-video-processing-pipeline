"""Code shared by both services: wire contracts, ports, adapters, settings, logging.

Deliberately contains no service-specific logic -- it is the seam that lets
VideoAnalyzer and StreamDetector run on different machines while agreeing on a
single message format.
"""
