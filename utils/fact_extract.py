import logging
from insights import extract, rule, make_metadata, run
from insights.util.canonical_facts import get_canonical_facts
from insights.core.archives import InvalidArchive
from insights.specs import Specs

logger = logging.getLogger('advisor-pup')

def _safe_parse(ds):
    try:
        return ds.content[0]

    except Exception:
        return None

@rule(optional=[Specs.meminfo])
def system_profile_facts(meminfo):
    return make_metadata(
        meminfo=_safe_parse(meminfo)
    )

def get_system_profile_facts(path=None):
    br = run(system_profile_facts, root=path)
    d = br[system_profile_facts]
    del d["type"]
    return d


async def extract_facts(archive):
    logger.info("extracting facts from %s", archive)
    facts = {}
    try:
        with extract(archive) as ex:
            facts = get_canonical_facts(path=ex.tmp_dir)
            facts['system_profile'] = get_system_profile_facts(path=ex.tmp_dir)
    except (InvalidArchive, ModuleNotFoundError, KeyError) as e:
        facts['error'] = e.args[0]

    return facts
