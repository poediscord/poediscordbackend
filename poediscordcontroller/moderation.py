from typing import Optional
from poediscordcontroller.model import MessageId, User
from poediscordcontroller.dispatch import Command, CommandHandler, CommandResult, handles_command
from poediscordcontroller import permissions

#@rest_api.route("users/{target}/ban")
@dataclass
class BanUser(Command):
    target: User
    initiator_response: MessageId
    reason: string
    until: Optional[datetime]

@permissions.require("ban user")
@handles_command(BanUser)
class BanUserHandler(CommandHandler):
    
    async def begin(self):
        if self.data.until:
            until_msg = f"until {self.data.until} "
        else:
            until_msg = ""
        await self.broker.dm_user(
            self.datadata.target,
            f"You have been banned {until_msg}because:\n{self.datadata.reason}")
        return self.apply_ban
        
    async def apply_ban(self):
        try:
            await self.broker.discord.apply_ban(
                self.data.target,
                self.data.reason, 
                self.data.until)
            return self.save_ban
            
        except DiscordExceptions.UserNotInServer:
            await self.schedule.on_user_login(self.data.cid)
            return CommandResult.Delayed
            
        except DiscordExceptions.UserAlreadyBanned:
            if self.data.initiator_response:
                await self.broker.update_response(
                    self.data.initiator_response,
                    f"Banning {command.target.name} failed because user already banned.")
            return CommandResult.Failed
        
    async def save_ban(self)
        await self.db.log_ban(
            command.initiator,
            command.target,
            command.reason,
            command.until)
        if self.data.initiator_response:
            return self.inform_initiator
        else:
            return CommandResult.Complete

    async def inform_initiator(self)
        await self.broker.update_response(
            self.data.initiator_response,
            f"Banned {command.target.name} ({command.target.did})!")
        return CommandResult.Complete
        