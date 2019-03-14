import logging
import ipaddress
from ipaddress import AddressValueError

from insights import extract, rule, make_metadata, run
from insights.util.subproc import CalledProcessError

from insights.core.archives import InvalidArchive
from insights.parsers.dmidecode import DMIDecode
from insights.parsers.installed_rpms import InstalledRpms
from insights.parsers.lsmod import LsMod
from insights.parsers.meminfo import MemInfo
from insights.parsers.redhat_release import RedhatRelease
from insights.parsers.uname import Uname
from insights.parsers.systemd.unitfiles import UnitFiles
from insights.parsers.virt_what import VirtWhat
from insights.specs import Specs
from insights.util.canonical_facts import get_canonical_facts, IPs

logger = logging.getLogger('advisor-pup')


@rule(optional=[Specs.hostname, Specs.lscpu, VirtWhat, MemInfo, IPs, DMIDecode, RedhatRelease, Uname, LsMod, InstalledRpms, UnitFiles])
def system_profile_facts(hostname, lscpu, virt_what, meminfo, ips, dmidecode, redhat_release, uname, lsmod, installed_rpms, unit_files):
    """
    System Properties:
      hostnames (list of just fqdn for now)
      mem in GB
    Infrastructure:
      infrastructure_type (phys or virt)
      infrastructure vendor (hypervisor type)
      IPv4 addresses (list)
      IPv6 addresses (list)
    BIOS:
      Vendor
      Version
      Release Date
    Operating System:
      Release (RHEL/Fedora)
      Kernel Version
      Kernel Release
      Arch
      Kernel Modules (list)
    Configuration
      Services
    """
    metadata_args = {}

    metadata_args['system_properties.hostnames'] = [_safe_parse(hostname)]
    metadata_args['system_properties.memory_in_gb'] = bytes_to_gb(meminfo.total) if meminfo else None

    metadata_args['infrastructure.type'] = _get_virt_phys_fact(virt_what)
    metadata_args['infrastructure.vendor'] = virt_what.generic if virt_what else None

    if ips:
        ip_addresses = ipv4_ipv6_addresses(ips.data)
        metadata_args['infrastructure.ipv4_addresses'] = ip_addresses.get('ipv4')
        metadata_args['infrastructure.ipv4_addresses'] = ip_addresses.get('ipv6')

    metadata_args['bios.vendor'] = dmidecode.bios_vendor if dmidecode else None
    metadata_args['bios.version'] = dmidecode.bios.get('version') if dmidecode else None
    # note that this is a date and not a datetime, hence no full iso8601
    metadata_args['bios.release_date'] = dmidecode.bios_date.isoformat() if dmidecode else None

    metadata_args['os.release'] = redhat_release.product if redhat_release else None
    metadata_args['os.kernel_version'] = uname.version if uname else None
    metadata_args['os.kernel_release'] = uname.release if uname else None
    metadata_args['os.arch'] = uname.arch if uname else None
    metadata_args['os.kernel_modules'] = list(lsmod.data.keys()) if lsmod else []  # convert for json serialization

    metadata_args['configuration.services'] = _create_services_fact(unit_files) if unit_files else None

    return make_metadata(**metadata_args)


def _get_virt_phys_fact(virt_what):
    if getattr(virt_what, 'is_virtual', False):
        return 'virtual'
    elif getattr(virt_what, 'is_physical', False):
        return 'physical'
    else:
        return None


def _create_services_fact(unit_files):
    """
    create a json serializable dict of services. The key is the service name,
    and the value is if it's enabled or not.
    """
    services = {}
    for service in unit_files.services:
        if service.endswith('.service'):
            services.update({service: unit_files.services[service]})

    return services


def test_ipv4_addr(address):
    """
    return true if address is ipv4, false otherwise
    """
    try:
        ipaddress.IPv4Address(address)
        return True
    except AddressValueError:
        return False


def test_ipv6_addr(address):
    """
    return true if address is ipv6, false otherwise
    """
    try:
        ipaddress.IPv6Address(address)
        return True
    except AddressValueError:
        return False


def ipv4_ipv6_addresses(addresses):
    """
    return a dict with IPv4 and IPv6 addresses
    """
    result = {'ipv4_addresses': [a for a in addresses if test_ipv4_addr(a)],
              'ipv6_addresses': [a for a in addresses if test_ipv6_addr(a)]}
    return result


def bytes_to_gb(num_bytes):
    return '%.1f' % float(num_bytes / 1_000_000_000) + " GB"


# from insights-core get_canonical_facts util
def _safe_parse(ds):
    try:
        return ds.content[0]

    except Exception:
        return None


def _strip_empty_facts(facts):
    defined_facts = {}
    for fact in facts:
        if facts[fact]:
            defined_facts.update({fact: facts[fact]})
    return defined_facts


def get_system_profile_facts(path=None):
    broker = run(system_profile_facts, root=path)
    result = broker[system_profile_facts]
    del result["type"]  # drop metadata key
    return result


def extract_facts(archive):
    logger.info("extracting facts from %s", archive)
    facts = {}
    try:
        with extract(archive) as ex:
            facts = get_canonical_facts(path=ex.tmp_dir)
            facts['system_profile'] = get_system_profile_facts(path=ex.tmp_dir)
    except (InvalidArchive, ModuleNotFoundError, KeyError, CalledProcessError) as e:
        facts['error'] = e.args[0]

    return _strip_empty_facts(facts)
