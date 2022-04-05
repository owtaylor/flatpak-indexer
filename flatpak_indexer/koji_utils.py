import koji


from .base_config import BaseConfig


class KojiConfig(BaseConfig):
    koji_config: str


def get_koji_session(config: KojiConfig):
    options = koji.read_config(profile_name=config.koji_config)
    koji_session_opts = koji.grab_session_options(options)
    return koji.ClientSession(options['server'], koji_session_opts)
