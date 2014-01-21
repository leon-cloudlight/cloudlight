# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 CloudLight, Inc
# All Rights Reserved.

import sys

from oslo.config import cfg

from mars.common import config
from mars.engines import engine


def main():
    """Main entry point of Mars Engines
    """
    config.parse(sys.argv[1:])
    config.setup_logging(cfg.CONF)
    if not cfg.CONF.config_file:
        sys.exit(_("ERROR: Unable to find configuration file via the default"
                   " search paths (~/.mars/, ~/, /etc/mars/, /etc/) and"
                   " the '--config-file' option!"))

    try:
        mars_engine = engine.createEngineFactory()
        mars_engine.start()
    except RuntimeError as e:
        mars_engine.stop()
        sys.exit(_("ERROR: %s") % e)


if __name__ == "__main__":
    main()


