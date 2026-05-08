"""Tests for the CLI module."""

import sys

import pytest


class TestCLIImport:
    def test_cli_module_importable(self):
        from aws2lakehouse import cli
        assert hasattr(cli, "main")

    def test_cli_has_subcommands(self):
        from aws2lakehouse.cli import main
        # main should be callable
        assert callable(main)


class TestCLIHelp:
    def test_help_exits_zero(self):
        """aws2lakehouse --help should exit 0."""
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse", "--help"]
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        finally:
            sys.argv = old_argv

    def test_version_flag(self):
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse", "--version"]
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        finally:
            sys.argv = old_argv

    def test_no_args_shows_help(self):
        """Running without subcommand should exit 1."""
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse"]
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
        finally:
            sys.argv = old_argv


class TestScanCommand:
    def test_scan_nonexistent_source(self):
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse", "scan", "--source", "/nonexistent/path"]
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
        finally:
            sys.argv = old_argv

    def test_scan_valid_source(self, sample_aws_project, capsys):
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse", "scan", "--source", sample_aws_project]
        try:
            main()
        finally:
            sys.argv = old_argv
        captured = capsys.readouterr()
        assert "Scanned:" in captured.out

    def test_scan_with_inventory(self, sample_aws_project, capsys):
        from aws2lakehouse.cli import main
        old_argv = sys.argv
        sys.argv = ["aws2lakehouse", "scan", "--source", sample_aws_project, "--inventory"]
        try:
            main()
        finally:
            sys.argv = old_argv
        captured = capsys.readouterr()
        assert "Inventory:" in captured.out
