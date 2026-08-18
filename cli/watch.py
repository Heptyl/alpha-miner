"""USER-scoped watchlist preferences; never writes the market database."""

from __future__ import annotations

from pathlib import Path

import click

from src.data.user_preferences import (
    UserPreferenceError,
    add_watch,
    load_watchlist,
    remove_watch,
    validate_preferences_path,
    validate_stock_code,
)

DEFAULT_PREFERENCES = Path("data/user_preferences.db")
def _code(_context: click.Context, _parameter: click.Parameter, value: str) -> str:
    try:
        return validate_stock_code(value)
    except UserPreferenceError as exc:
        raise click.BadParameter(str(exc)) from exc
@click.group()
@click.option("--db", "db_path", type=click.Path(path_type=Path), default=DEFAULT_PREFERENCES)
@click.pass_context
def main(context: click.Context, db_path: Path) -> None:
    """管理独立个人自选；不会修改市场事实或研究排名。"""
    context.obj = validate_preferences_path(db_path)
@main.command("add")
@click.argument("stock_code", callback=_code)
@click.pass_obj
def add_cmd(db_path: Path, stock_code: str) -> None:
    created = add_watch(db_path, stock_code)
    click.echo(f"{'已添加' if created else '已存在'}：{stock_code}")
@main.command("remove")
@click.argument("stock_code", callback=_code)
@click.pass_obj
def remove_cmd(db_path: Path, stock_code: str) -> None:
    removed = remove_watch(db_path, stock_code)
    click.echo(f"{'已移除' if removed else '未找到'}：{stock_code}")
@main.command("list")
@click.pass_obj
def list_cmd(db_path: Path) -> None:
    items = load_watchlist(db_path)
    if not items:
        click.echo("自选为空")
        return
    for stock_code, added_at in items:
        click.echo(f"{stock_code}  添加于 {added_at}")
if __name__ == "__main__":
    main()
