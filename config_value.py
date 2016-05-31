import argparse
import configparser

def main():
    """ main """

    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", action="store",
                        help="Config file with settings",
                        type=file, required=True)
    parser.add_argument("--section-name", type=str, required=True)
    parser.add_argument("--key-name", type=str, required=True)
    parser.add_argument("-as-boolean", required=False, action='store_true')

    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    config.read_file(args.config_file)

    if args.as_boolean:
        print config[args.section_name].getboolean(args.key_name)
    else:
        print config[args.section_name][args.key_name]

if __name__ == "__main__":
    main()
