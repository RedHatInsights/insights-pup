from prometheus_client import start_http_server, Info, Counter, Gauge, Summary # noqa

total = Counter('pup_advisor_total', 'The total amount of uploads')
valid = Counter('pup_advisor_valid', 'The total amount of valid uploads')
invalid = Counter('pup_advisor_invalid', 'The total amount of successfully validated uploads')
inventory_post_success = Counter('pup_advisor_inventory_post_success', 'The total amount of successful inventory posts')
inventory_post_failure = Counter('pup_advsior_inventory_post_failure', 'The total amount of failed inventory posts')

validation_time = Summary('validation_processing_seconds', 'Time spent validating archive')
inventory_post_time = Summary('inventory_post_processing_seconds', 'Time spent posting to inventory')

upload_service_version = Info('upload_service_version', 'Build commit and date')

produce_queue_size = Gauge("pup_produce_queue_size", "Size of the produce queue")
system_profile_queue_size = Gauge("pup_system_profile_queue_size", "Size of the system profile queue")
current_archives_size = Gauge("pup_current_archives_size", "Number of archives processing currently")
