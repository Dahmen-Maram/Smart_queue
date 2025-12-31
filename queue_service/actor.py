from dapr.actor import Actor, ActorInterface, actormethod
from dapr.actor.runtime.config import ActorRuntimeConfig

class IPatientActor(ActorInterface):
    @actormethod(name="set_position")
    async def set_position(self, position: int):
        pass

    @actormethod(name="get_position")
    async def get_position(self) -> int:
        pass

class PatientActor(Actor, IPatientActor):
    def __init__(self, ctx, actor_id):
        super().__init__(ctx, actor_id)
        self._position = 0

    async def set_position(self, position: int):
        self._position = position

    async def get_position(self) -> int:
        return self._position

ActorRuntimeConfig(actor_idle_timeout="1h", actor_scan_interval="30s")
