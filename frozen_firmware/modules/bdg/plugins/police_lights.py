import asyncio
import random
from time import time
from machine import Pin
from neopixel import NeoPixel

from bdg.msg import BadgeMsg, send_message
from bdg.msg.connection import Beacon
from bdg.utils import AProc
from bdg.bleds import clear_leds, dimm_gamma


@BadgeMsg.register
class PoliceBroadcast(BadgeMsg):
    def __init__(self, color=None, seq=None):
        super().__init__()
        self.color = color
        self.seq = seq


LED_RED = dimm_gamma([(255, 0, 0)], 0.4)[0]
LED_BLUE = dimm_gamma([(0, 0, 255)], 0.4)[0]
LED_OFF = (0, 0, 0)
BROADCAST_MAC = b"\xbb\xbb\xbb\xbb\xbb\xbb"
REBROADCAST_DELAY_MS = 500
IDLE_RETRY_S = 3


class PolicePlugin(AProc):
    __espnow = None
    _msg_queue = None
    _last_seq = 0
    _last_activity = 0

    @classmethod
    def setup(cls, espnow):
        print("PolicePlugin: setup")
        cls.__espnow = espnow
        cls._last_seq = 0
        cls._last_activity = time()
        try:
            from primitives import Queue
            cls._msg_queue = Queue(maxsize=5)
            print("PolicePlugin: setup complete")
        except Exception as e:
            print(f"PolicePlugin: queue init failed: {e}")

    @classmethod
    def enqueue(cls, msg):
        if cls._msg_queue is None:
            return
        print(f"PolicePlugin: enqueued msg color={msg.color} seq={msg.seq}")
        try:
            cls._msg_queue.put_nowait(msg)
        except Exception:
            print("PolicePlugin: queue full, dropping message")

    @classmethod
    def trigger(cls, color="red"):
        if cls.__espnow is None:
            return
        cls._last_seq += 1
        seq = cls._last_seq
        print(f"PolicePlugin: trigger color={color} seq={seq}")
        msg = PoliceBroadcast(color=color, seq=seq)
        asyncio.create_task(cls._flash(color))
        asyncio.create_task(cls._send(msg))

    @classmethod
    async def task(cls, *args, **kwargs):
        print("PolicePlugin: started")

        while not cls.stop_event.is_set():
            # Check for idle timeout — re-trigger if no activity
            if time() - cls._last_activity > IDLE_RETRY_S:
                if Beacon._susp.is_set():
                    jitter = random.randint(0, 3)
                    print(f"PolicePlugin: idle for {IDLE_RETRY_S}s, retrying in {jitter}s")
                    await asyncio.sleep(jitter)
                    cls._last_seq += 1
                    cls._last_activity = time()
                    msg = PoliceBroadcast(color="red", seq=cls._last_seq)
                    await cls._send(msg)
                    continue

            try:
                msg = await asyncio.wait_for(cls._msg_queue.get(), 1.0)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"PolicePlugin: queue error: {e}")
                continue

            if msg.seq is None or msg.seq <= cls._last_seq:
                print(f"PolicePlugin: ignoring stale msg seq={msg.seq} last_seq={cls._last_seq}")
                continue

            if not Beacon._susp.is_set():
                print("PolicePlugin: skipping, beacon suspended (game active)")
                continue

            cls._last_seq = msg.seq
            cls._last_activity = time()

            next_color = "blue" if msg.color == "red" else "red"
            print(f"PolicePlugin: received {msg.color} seq={msg.seq}, flashing {next_color}")
            await cls._flash(next_color)
            await asyncio.sleep_ms(REBROADCAST_DELAY_MS)

            cls._last_seq += 1
            reply = PoliceBroadcast(color=next_color, seq=cls._last_seq)
            print(f"PolicePlugin: rebroadcasting {next_color} seq={cls._last_seq}")
            await cls._send(reply)

    @classmethod
    async def _flash(cls, color):
        print(f"PolicePlugin: flashing {color}")
        led_power = Pin(17, Pin.OUT)
        led_power.value(1)
        np = NeoPixel(Pin(18), 10)
        led_color = LED_RED if color == "red" else LED_BLUE

        for _ in range(3):
            for i in range(np.n):
                np[i] = led_color
            np.write()
            await asyncio.sleep_ms(80)

            for i in range(np.n):
                np[i] = LED_OFF
            np.write()
            await asyncio.sleep_ms(60)

        clear_leds(np)
        led_power.value(0)

    @classmethod
    async def _send(cls, msg):
        try:
            print(f"PolicePlugin: sending broadcast color={msg.color} seq={msg.seq}")
            await send_message(cls.__espnow, BROADCAST_MAC, msg.srlz(), sync=False)
            print("PolicePlugin: broadcast sent")
        except Exception as e:
            print(f"PolicePlugin: send failed: {e}")
