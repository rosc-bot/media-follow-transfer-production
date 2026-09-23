import pytest

from app.bot.main import bot_commands, register_command_menu


class RecordingBot:
    def __init__(self):
        self.calls = []

    async def set_my_commands(self, commands, scope):
        self.calls.append((commands, scope))


@pytest.mark.asyncio
async def test_bot_registers_consolidated_private_command_menu():
    bot = RecordingBot()

    await register_command_menu(bot)

    assert [command.command for command in bot_commands()] == [
        'start', 'hot', 'calendar', 'radar', 'follow', 'queue', 'scan', 'sync', 'pause', 'resume', 'settings', 'admins', 'help'
    ]
    assert len(bot.calls) >= 1
    commands, scope = bot.calls[0]
    assert [command.command for command in commands] == [
        'start', 'hot', 'calendar', 'radar', 'follow', 'queue', 'scan', 'sync', 'pause', 'resume', 'settings', 'admins', 'help'
    ]
    assert scope.__class__.__name__ == 'BotCommandScopeAllPrivateChats'
