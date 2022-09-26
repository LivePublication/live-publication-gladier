from abc import ABC, abstractmethod
from packaging import version
import logging
import gladier.version

log = logging.getLogger(__name__)


class ConfigMigration(ABC):

    def __init__(self, config):
        self.config = config

        if 'general' not in self.config.sections():
            log.debug('No version in config, adding...')
            self.config.add_section('general')
        cfg_version = self.config['general'].get('version')

        self.config_version = version.parse(cfg_version) if cfg_version else None
        self.version = version.parse(gladier.version.__version__)

    @abstractmethod
    def is_applicable(self):
        pass

    @abstractmethod
    def migrate(self):
        pass


class AddVersionToConfig(ConfigMigration):
    """
    Add a version if one does not exist in the config.
    """
    def is_applicable(self):
        return self.config_version is None

    def migrate(self):
        # We can't be certain which version of Gladier the user was using previously.
        # It's possible they simply upgraded from 3 to 4 and are using incompatible
        # funcx functions. Run the migration here just in case.
        migrate_delete_all_funcx_functions(self.config)

        # Set the version
        self.config['general']['version'] = str(self.version)
        log.info(f'Setting Version {self.version}')


class UpdateConfigVersion(ConfigMigration):
    """Sets the config version to the current version of Gladier.
    Should be run LAST, after all other migrations have taken place"""
    def is_applicable(self):
        return self.config_version != self.version

    def migrate(self):
        self.config['general']['version'] = str(self.version)


MIGRATIONS = [
    AddVersionToConfig,
    UpdateConfigVersion,
]


def needs_migration(config):
    return any(m(config).is_applicable() for m in MIGRATIONS)


def migrate_gladier(config):
    for migration in MIGRATIONS:
        m_instance = migration(config)
        if m_instance.is_applicable():
            log.info(f'Applying migration: {m_instance.__class__.__name__}')
            m_instance.migrate()
    return config


def panic_print(message):
    """Print a message to console for the user to see. The message must be URGENT."""
    print(message)


def migrate_delete_all_funcx_functions(config):
    """FuncX changed servers from v0.0.5 to v0.2.4/v0.3.0. Simply delete all functions
    to upgrade to the latest version. The new version of funcx will re-register them."""
    for section in config.sections():
        for option in config[section]:
            if option.endswith('_funcx_id') or option.endswith('_funcx_id_checksum'):
                log.debug(f'Deleting {option}')
                del config[section][option]