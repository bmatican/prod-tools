#! /usr/bin/python

import argparse
import itertools
import os
import re
import subprocess

YB_INSTALL_DIR_VAR = "YB_INSTALL_DIR"
def get_yb_install_dir():
    yb_install_dir = os.environ.get(YB_INSTALL_DIR_VAR)
    if yb_install_dir is None:
        raise RuntimeError("Must set env var: YB_INSTALL_DIR")
    return yb_install_dir


def get_binary_path(binary_name):
    yb_install_dir = get_yb_install_dir()
    file_path = os.path.join(yb_install_dir, "bin", binary_name)
    if not os.path.isfile(file_path):
        raise RuntimeError("Invalid YB_INSTALL_DIR: {}".format(yb_install_dir))
    return file_path


class Config():
    def __init__(self, conf_file_path):
        f = open(conf_file_path, 'r')
        self.flags = {}
        for line in f.readlines():
            line = line.strip()
            if re.match("^#.*", line):
                # print("Found comment line: {}".format(line))
                continue
            else:
                match = re.match("^--(.*)=(.*)$", line)
                if not match:
                    raise RuntimeError("Found invalid config line: {}".format(line))
                self.flags[match.group(1)] = match.group(2)
        # print(self.flags)

    def get_data_dirs(self):
        return self.flags["fs_data_dirs"].strip().split(",")


class Operations():
    def __init__(self):
        self.args = None

    def start_server(self):
        config = Config(self.args.conf)
        binary_path = get_binary_path("yb-{}".format(self.args.process))
        first_data_dir = config.get_data_dirs()[0]
        command = "{0} --flagfile {1} >{2}/{3}.out 2>{2}/{3}.err &".format(
            binary_path, self.args.conf, first_data_dir, self.args.process)
        print("Starting server: {}".format(command))
        os.system(command)

    def stop_server(self):
        print("Stopping server: yb-{}".format(self.args.process))
        os.system("pkill -f yb-{}".format(self.args.process))

    def drain_start(self):
        ts_list = self.validate_ip_port_csv(self.args.tservers)
        start_cmd_list = [
            "change_blacklist", "ADD"
        ]
        start_cmd_list.extend(ts_list)
        self.run_yb_admin_command(start_cmd_list)
        self.print_cluster_config()

    def drain_stop(self):
        ts_list = self.validate_ip_port_csv(self.args.tservers)
        stop_cmd_list = [
            "change_blacklist", "REMOVE"
        ]
        stop_cmd_list.extend(ts_list)
        self.run_yb_admin_command(stop_cmd_list)
        self.print_cluster_config()

    def validate_ip_port_csv(self, servers):
        ip_port_list = servers.split(",")
        for ip_port in ip_port_list:
            if ":" not in ip_port:
                raise RuntimeError("Invalid format for TS: {}".format(ip_port))
        return ip_port_list

    def drain_status(self):
        output = self.run_yb_admin_command(["get_load_move_completion"])
        _, percent = output.strip().split("=")
        print(percent.strip())

    def print_cluster_config(self):
        print(self.run_yb_admin_command(["get_universe_config"]))

    def run_yb_admin_command(self, arg_list):
        yb_admin_bin = get_binary_path("yb-admin")
        cmd_list = [
            yb_admin_bin, "-master_addresses", self.args.master_addresses
        ]
        cmd_list.extend(arg_list)
        print("Running yb-admin command: {}".format(" ".join(cmd_list)))
        # return subprocess.check_output(cmd_list)

    def move_masters(self):
        old_master_ip_ports = self.validate_ip_port_csv(self.args.master_addresses)
        new_master_ip_ports = self.validate_ip_port_csv(self.args.new_master_addresses)
        zipped = list(itertools.izip_longest(old_master_ip_ports, new_master_ip_ports))
        for old, new in zipped:
            base_cmd = ["change_master_config"]
            if new is not None:
                cmd_list = base_cmd + ["ADD_SERVER"] + new.split(":")
                self.run_yb_admin_command(cmd_list)
            if old is not None:
                cmd_list = base_cmd + ["REMOVE_SERVER"] + old.split(":")
                self.run_yb_admin_command(cmd_list)
        new_master_ips = [e.split(":")[0] for e in new_master_ip_ports]
        print("")
        print("Now remember to change the master addresses in the config files on the new nodes!")
        print("")
        print("Modify the master.conf for the new masters: {}".format(" ".join(new_master_ips)))
        print("--master_addresses={}".format(self.args.new_master_addresses))
        print("")
        print("Modify the tserver.conf for all the new servers!")
        print("--tserver_master_addrs={}".format(self.args.new_master_addresses))


def main():
    operations = Operations()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    # Start process.
    start_parser = subparsers.add_parser("start", help="Start a YugaByte process")
    start_parser.add_argument(
        "process", choices=["master", "tserver"],
        help="Which server do you want to start")
    start_parser.add_argument(
        "--conf", required=True,
        help="Path to server config file.")
    start_parser.set_defaults(func=operations.start_server)

    # Stop process.
    stop_parser = subparsers.add_parser("stop", help="Stop a YugaByte process")
    stop_parser.add_argument(
        "process", choices=["master", "tserver"],
        help="Which server do you want to start")
    stop_parser.set_defaults(func=operations.stop_server)

    # Admin commands.
    admin_parser = subparsers.add_parser("admin", help="Stop a YugaByte process")
    admin_parser.add_argument(
        "--master_addresses", type=str, required=True,
        help="CSV of master addresses for a cluster to operate on")
    admin_commands = admin_parser.add_subparsers()

    # Drain commands.
    drain_parser = admin_commands.add_parser(
        "drain", help="Commands for managing traffic drain")
    drain_commands = drain_parser.add_subparsers()
    drain_start_parser = drain_commands.add_parser(
        "start", help="Start draining traffic from target nodes")
    drain_start_parser.add_argument(
        "--tservers", required=True,
        help="CSV of tserver info in IP:PORT format")
    drain_start_parser.set_defaults(func=operations.drain_start)

    drain_stop_parser = drain_commands.add_parser(
        "stop", help="Stop draining traffic from target nodes and clear metadata")
    drain_stop_parser.add_argument(
        "--tservers", required=True,
        help="CSV of tserver info in IP:PORT format")
    drain_stop_parser.set_defaults(func=operations.drain_stop)

    drain_status_parser = drain_commands.add_parser(
        "status", help="Check status of current traffic draining")
    drain_status_parser.set_defaults(func=operations.drain_status)

    # Move masters
    move_masters_parser = admin_commands.add_parser("move-masters", help="Stop a YugaByte process")
    move_masters_parser.add_argument(
        "--new_master_addresses", required=True,
        help="New master addresses to move to")
    move_masters_parser.set_defaults(func=operations.move_masters)


    operations.args = parser.parse_args()
    operations.args.func()


if __name__ == "__main__":
    main()
