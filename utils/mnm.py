from prometheus_client import start_http_server, Counter # noqa

total = Counter('pup_advisor_total', 'The total amount of uploads')
valid = Counter('pup_advisor_valid', 'The total amount of valid uploads')
invalid = Counter('pup_advisor_invalid', 'The total amount of successfully validated uploads')
inventory_post_success = Counter('pup_advisor_inventory_post_success', 'The total amount of successful inventory posts')
inventory_post_failure = Counter('pup_advsior_inventory_post_failure', 'The total amount of failed inventory posts')
