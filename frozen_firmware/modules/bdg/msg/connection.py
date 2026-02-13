import asyncio
from time import ticks_ms, ticks_diff, time

import aioespnow
from collections import namedtuple, deque

from bdg.msg import (
    OpenConn,
    send_message,
    ConTerm,
    PingMsg,
    AppMsg,
    BadgeMsg,
    BeaconMsg,
    BadgeAdr,
    BadgeAdrDict,
    AckMsg,
)

from bdg.utils import AProc
from primitives import Queue


OutQueMsg = namedtuple("OutQueMsg", ["msg", "mac", "id", "retry"])
OutQueAck = namedtuple("OutQueMsg", ["mac", "id"])


class Connection(object):
    """
    Connection is a bidirectional communication channel between two badges.

    Attributes:
        c_mac (bytes): MAC address of the recipient
        espnow: The ESP-NOW instance used for communication.
        active (bool): Status of whether the connection is active or not.
        last_msg (timestamp): Timestamp of the last message received.
        con_id: Unique identifier for the app that uses this connection. Like content-type
        in_q (Queue): Queue to store incoming messages.

    Methods:
        async connect(self, rcvr=False):
            Initiates a connection or responds to a connection request. Returns True if successful, False otherwise.

        async terminate(self):
            Terminates the connection by sending a termination message and setting the connection to inactive.

        async ping(self):
            Sends a ping message and waits for a reply.

        async recv_msg(self, msg: BadgeMsg):
            Handles the reception of messages internally and processes different types of messages. Called by NowListener.

        async send_app_msg(self, msg: BadgeMsg, sync=False):
            Sends an application message over the connection. Receiving end gets the same class as the sender sent.

        async send_msg_b(self, msg: bytes, sync=False):
            Sends a byte message over the connection.

        async send_wait_reply(self, msg: bytes, sync=False, timeout=5.0):
            Sends a message and waits for a reply within a timeout period. Raises TimeoutError if timeout exceeded.

        get_msg_aiter(self):
            Returns an asynchronous iterator to iterate over incoming messages.
    """

    # Connection is a bidirectional communication channel between two badges
    #
    def __init__(self, mac: bytes, con_id, espnow):
        self._sender_t: asyncio.Task = None
        self.espnow: espnow = espnow
        self.c_mac: bytes = mac
        # self.call_chnl = 1
        # self.talk_chnl = 3
        self.active = False
        self.closed = False
        self.last_msg = time()
        self.con_id = con_id
        self.session_id = ticks_ms()  # unique session ID to prevent cross-session messages
        self.in_q = Queue(maxsize=5)
        self.out_q = Queue(maxsize=3)

        NowListener.register_con(self)

    def __del__(self):
        print("conn closed")

    async def terminate(self, send_out=True, reply_to_id=None):
        # send connection terminated to local listeners
        ct = ConTerm(con_id=self.con_id)
        self.in_q.put_nowait(ct)
        if send_out:
            if reply_to_id:
                ct.__id = reply_to_id
            self.send_msg(ct)
            NowListener.unregister_con(self)
        self.active = False
        self.closed = True

    async def connect(self, rcvr=False):
        try:
            oc = OpenConn(con_id=self.con_id, session_id=self.session_id)
            if rcvr:
                self.send_msg(oc)
                self.active = True
                return True
            else:
                reply = await self.send_wait_reply(oc, timeout=20)
            if (
                not isinstance(reply, OpenConn)
                or reply.accept is False
                or reply.con_id != self.con_id
            ):
                # print(f'not accepted reply: {type(reply)} {reply=}')
                return await self.terminate(False)
            # Store peer's session_id from their reply
            if hasattr(reply, 'session_id') and reply.session_id:
                self.session_id = reply.session_id
            # connection made
            self.active = True
            return True
        except asyncio.TimeoutError:
            await self.terminate()
            return False
        except Exception as err:
            print(f"conn err {err}")
            return False

    async def ping(self):
        print(f"ping: ")
        mark = ticks_ms()
        self.send_app_msg(PingMsg(mark, False), sync=False)
        reply = await asyncio.wait_for(self.in_q.get(), 5)
        print(f"ping reply: {ticks_diff(ticks_ms(), mark)}ms {reply=}")
        return reply

    async def _sender(self):
        # sender task to decouple sync calls to async.
        # micro gui callbacks are sync so this is needed
        # if in async context self.send_app_msg can be called directly
        if not self.active:
            print(f"cannot send con {self.con_id} terminated")
            return  # cannot send on closed connection
        while self.out_q.qsize() > 0 or self.active:
            msg = await asyncio.wait_for(self.out_q.get(), 5000)
            print(f"_s:  {msg}")
            self.send_app_msg(msg, sync=False)

    async def recv_msg(self, msg: BadgeMsg):
        # internal recv_msg that is called from NowListener
        print(f"recv-msg {msg=}")
        if isinstance(msg, ConTerm):
            if self.active:
                await self.terminate(send_out=False)
                print(f"connection {self.con_id} terminated")
        elif isinstance(msg, OpenConn):
            if not self.active:
                # Store peer's session_id from their OpenConn
                if hasattr(msg, 'session_id') and msg.session_id:
                    self.session_id = msg.session_id
                self.in_q.put_nowait(msg)
                self.active = True
                print(f"connection {self.con_id} activated, session_id={self.session_id}")
            # self.send_msg(AckMsg(id=msg.id), retry=0)
        elif isinstance(msg, PingMsg):
            if msg.reply:
                self.in_q.put_nowait(msg)
                return
            msg.reply = True
            self.send_app_msg(msg)
        elif not self.active:
            print("connection not active")
        else:
            # this can block, should check quefull and return something to client B
            self.in_q.put_nowait(msg)

    def send_app_msg(self, msg: BadgeMsg, sync=False):
        amsg = AppMsg(con_id=self.con_id, content=msg, session_id=self.session_id)
        if self.closed:
            print(f"cannot send {self.con_id=} is terminated")
            return  # cannot send on closed connection
        NowListener.send_msg(amsg, self.c_mac, sync=sync)

    def send_msg(self, msg: BadgeMsg, sync=False, retry=3):
        if self.closed:
            print(f"cannot send {self.con_id=} is terminated")
            return  # cannot send on closed connection # TODO :raise
        NowListener.send_msg(msg, self.c_mac, sync=sync, retry=retry)

    async def send_wait_reply(self, msg: BadgeMsg, sync=False, timeout=5.0):
        # raises TimeoutError if timeout exceeded
        self.send_msg(msg, sync=sync)
        return await asyncio.wait_for(self.in_q.get(), timeout)

    def get_msg_aiter(self):
        class Aiter:
            def __init__(self, conn: Connection):
                self.conn = conn

            def __aiter__(self):  # See note below
                return self

            async def __anext__(self):
                msg: AppMsg = await self.conn.in_q.get()
                print(f"__anext__ ")
                if isinstance(msg, ConTerm):
                    raise StopAsyncIteration
                self.conn.last_msg = time()
                return msg

        return Aiter(self)


async def def_con_cb(con: Connection, req=False):
    """
    Callback for handling incoming connections.

    Args:
        con (Connection): The incoming connection instance.
        :param req: Incoming conn or self made request
    """
    if not req:
        print(f"Incoming connection {con.con_id}")
    else:
        print(f"Connect request {con.con_id}")
    return True


def wait_index(msg):
    return msg.mac + bytes([msg.id])


def wait_index_mac(mac, msg_id):
    return mac + bytes([msg_id])


class NowListener(object):
    """
    The NowListener class listens and processes incoming ESP-NOW messages. It manages connections,
    handles incoming messages, and maintains an update mechanism for the seen devices.

    This class is designed to function as a singleton to ensure that only one instance handles
    the ESP-NOW communication. It provides mechanisms for registering, unregistering, and dispatching
    messages to connections.

    Attributes:
        __instance (NowListener): Singleton instance of the class.
        connections (dict): Dictionary holding active connections indexed by connection ID.
        last_seen (BadgeAdrDict): Dict like object with eviction after max_size reached
        update_event (asyncio.Event): Asyncio event to notify updates.
        conn_request (asyncio.Event): Asyncio event for new connection requests.
        __espnow (aioespnow.AIOESPNow): AIOESPNow instance to handle ESP-NOW communication.

    Methods:
        incoming_con_cb(con): Callback for handling incoming connections.
        task(): Main task to listen and process incoming ESP-NOW messages.
        get_updates(): Returns a generator that yields the last seen updates.
        register_con(connection): Registers a new connection and adds the respective peer in ESP-NOW.
        unregister_con(connection): Unregisters a connection and removes it from the active connections.
        start(espnow): Starts the NowListener instance if not already started.
        stop(): Stops the NowListener instance if it is running.
        dispatch_app_msg(app_msg): Dispatches an application message to the corresponding connection.
        dispatch_msg(msg, con_id): Dispatches a message to the corresponding connection based on connection ID.
    """

    __task = None
    __instance = None
    __cleanup_task = None
    _sender_t = None
    connections = {}
    delivered = deque([], 50)  # Track last 50 messages to prevent re-delivery
    last_seen = BadgeAdrDict(max_size=20, stale_multiplier=2.6)

    update_event = asyncio.Event()
    conn_request = asyncio.Event()
    out_q = Queue(maxsize=5)

    __espnow: aioespnow.AIOESPNow = None
    con_cb = def_con_cb
    
    # Malformed message tracking: {mac: (count, first_timestamp)}
    malformed_counter = {}
    # Blocked MACs: {mac: block_expiry_timestamp}
    blocked_macs = {}

    def __init__(self, e, con_cb=None):
        if not NowListener.__espnow:
            NowListener.__espnow = e
        if con_cb:
            NowListener.con_cb = con_cb

    def _track_malformed_message(self, mac):
        """Track malformed messages and block MAC if threshold exceeded."""
        current_time = time()
        mac_hex = ":" .join(f"{byte:02x}" for byte in mac)
        
        if mac in NowListener.malformed_counter:
            count, first_time = NowListener.malformed_counter[mac]
            
            # Reset counter if more than 10 seconds have passed
            if current_time - first_time > 10:
                NowListener.malformed_counter[mac] = (1, current_time)
            else:
                count += 1
                NowListener.malformed_counter[mac] = (count, first_time)
                
                # Block if threshold exceeded (3 malformed in 10 seconds)
                if count >= 3:
                    block_until = current_time + 30  # Block for 30 seconds
                    NowListener.blocked_macs[mac] = block_until
                    print(f"Blocking MAC {mac_hex} for 30s (>= 3 malformed msgs)")
        else:
            NowListener.malformed_counter[mac] = (1, current_time)
    
    def ack_msg(self, mac, msg_id):
        self.out_q.put_nowait(OutQueAck(mac, msg_id))
        # start sender task to eat the out_q
        if self._sender_t is None or self._sender_t.done():
            self._sender_t = asyncio.create_task(self._sender())

    async def cleanup_task(self):
        """Periodically cleanup stale badges from last_seen and blocked MACs."""
        try:
            while True:
                await asyncio.sleep(5)  # Check every 5 seconds
                removed = NowListener.last_seen.cleanup_stale(Beacon.timeout)
                if removed > 0:
                    print(f"Cleaned up {removed} stale badge(s)")
                    self.update_event.set()  # Notify UI to update
                
                # Cleanup expired blocked MACs
                current_time = time()
                expired_blocks = [mac for mac, expiry in NowListener.blocked_macs.items() if current_time > expiry]
                for mac in expired_blocks:
                    del NowListener.blocked_macs[mac]
                    if mac in NowListener.malformed_counter:
                        del NowListener.malformed_counter[mac]
                    mac_hex = ":".join(f"{byte:02x}" for byte in mac)
                    print(f"Unblocked MAC {mac_hex} - block expired")
        except Exception as e:
            print(f"cleanup_task error: {e}")

    async def task(self):
        """
        Main task to listen and process incoming ESP-NOW messages.
        Handles different types of messages (BeaconMsg, OpenConn, ConTerm, AppMsg) and updates connections.
        """
        print("NowListener active")
        no_ack = 0
        async for mac, msg in self.__espnow:
            if mac is None:
                continue

            # Check if MAC is blocked
            if mac in NowListener.blocked_macs:
                if time() < NowListener.blocked_macs[mac]:
                    # Still blocked, silently ignore
                    continue
                else:
                    # Block expired, cleanup will handle removal
                    pass

            rssi = self.__espnow.peers_table[mac][0]

            # Protect deserialization so a malformed message doesn't cancel the listener
            try:
                incm_msg = BadgeMsg.desrlz(msg)
            except Exception as e:
                mac_hex = ":".join(f"{byte:02x}" for byte in mac)
                print(f"NowListener: fatal deserialization from {mac_hex}: {e}")
                self._track_malformed_message(mac)
                continue

            if incm_msg is None:
                mac_hex = ":".join(f"{byte:02x}" for byte in mac)
                head = msg[:32] if isinstance(msg, (bytes, bytearray)) else b""
                print(f"Ignoring malformed msg from {mac_hex} len={len(msg)} head={head.hex()}")
                self._track_malformed_message(mac)
                continue

            print(f">>>{mac}:{incm_msg}")

            if isinstance(incm_msg, BeaconMsg):
                NowListener.last_seen[mac] = BadgeAdr(mac, incm_msg.nick, rssi, time())
                self.update_event.set()  # trigger updates function
            elif incm_msg.msg_type == "PoliceBroadcast":
                try:
                    from bdg.plugins.police_lights import PolicePlugin
                    PolicePlugin.enqueue(incm_msg)
                except Exception as e:
                    print(f"NowListener: police plugin error: {e}")
            elif isinstance(incm_msg, AckMsg):
                NowListener.last_seen.update_last_seen(mac, time())
                # mark for retry buffer that msg is acked
                self.ack_msg(mac, incm_msg.id)

            elif isinstance(incm_msg, OpenConn):
                NowListener.last_seen.update_last_seen(mac, time())
                
                # Check if there's an existing connection for this con_id and MAC
                existing_conn = self.connections.get(incm_msg.con_id)
                if existing_conn and not existing_conn.closed:
                    # Check if this is from the same peer (reply to our connection request)
                    if existing_conn.c_mac == mac:
                        # This is a reply to our connection request, dispatch it
                        if await self.dispatch_msg(incm_msg, incm_msg.con_id, mac):
                            self.ack_msg(mac, incm_msg.id)
                            continue
                    else:
                        # Existing connection with different peer - reject new one
                        print(f"Rejecting OpenConn: con_id {incm_msg.con_id} already used by different peer")
                        await send_message(
                            self.__espnow, mac, 
                            OpenConn(incm_msg.con_id, accept=False).srlz(), 
                            sync=False
                        )
                        continue
                elif existing_conn and existing_conn.closed:
                    # Old closed connection still registered - clean it up
                    print(f"Cleaning up closed connection for con_id={incm_msg.con_id}")
                    NowListener.unregister_con(existing_conn)

                # Add new incoming connection, ack the incoming OpenConn
                await send_message(
                    self.__espnow, mac, AckMsg(id=incm_msg.id).srlz(), sync=False
                )

                # proto connection, not yet capable of receiving other messages
                conn = Connection(mac, incm_msg.con_id, self.__espnow)
                # Use session_id from incoming OpenConn if available
                if hasattr(incm_msg, 'session_id') and incm_msg.session_id:
                    conn.session_id = incm_msg.session_id
                conn.active = True

                try:
                    # ask user process can we accept connection
                    (await NowListener.con_cb(conn)) or 1 / 0
                except (asyncio.TimeoutError, ZeroDivisionError):
                    # connection was not opened in time, or it returned false
                    NowListener.unregister_con(conn)
                    await asyncio.sleep(0.1)  # Allow now esp stack to run
                    await conn.terminate()
                    continue

                # connection accepted, register to allow subsequent messages
                NowListener.register_con(conn)
                await asyncio.sleep(0.1)  # Allow now esp stack to run
                # Opening connection by replying OpenConn back with same msg id and session_id
                oc = OpenConn(incm_msg.con_id, accept=True, session_id=conn.session_id)
                oc.__id = incm_msg.id
                NowListener.send_msg(oc, mac)
                # await send_message(self.__espnow, mac, msg, sync=False)

            elif isinstance(incm_msg, ConTerm):
                self.ack_msg(mac, incm_msg.id)
                NowListener.last_seen.update_last_seen(mac, time())

                if incm_msg.con_id in self.connections:
                    print(f"con term for {incm_msg=}")
                    conn = self.connections[incm_msg.con_id]
                    await conn.terminate(send_out=True, reply_to_id=incm_msg.id)
                    NowListener.unregister_con(conn)
                else:
                    await send_message(
                        self.__espnow, mac, AckMsg(id=incm_msg.id).srlz(), sync=False
                    )

            elif isinstance(incm_msg, AppMsg):
                NowListener.last_seen.update_last_seen(mac, time())
                await send_message(
                    self.__espnow, mac, AckMsg(id=incm_msg.id).srlz(), sync=False
                )

                if not await self.dispatch_app_msg(incm_msg, mac):
                    print(f"No receiver for RCV:{mac}->{incm_msg=}")

            else:
                tmp = ":".join(f"{byte:02x}" for byte in mac)
                print(f"{tmp} [{rssi}dBm] {msg} :")
            await asyncio.sleep(
                0.1
            )  # Do not touch, MSG stack crashes when running without

    @classmethod
    def updates(cls, filter_mac=None):
        return cls.__instance.get_updates(filter_mac=filter_mac)

    def get_updates(self, filter_mac=None):
        """
        `get_updates` returns a generator that yields the latest BadgeAddr.

        Returns:
            generator: A generator that yields the last seen updates.
            filter_mac: bytes(6) mac return only updates to this mac
        """

        class Aiter:
            def __init__(self, scanner):
                self.scanner = scanner

            def __aiter__(self):
                return self

            async def __anext__(self):
                while True:
                    await self.scanner.update_event.wait()
                    self.scanner.update_event.clear()
                    latest = self.scanner.last_seen.latest()
                    if filter_mac is None or filter_mac == latest.mac:
                        return latest

        return Aiter(self)

    async def _sender(self):
        # temporary task to send messages for retry times or until ack arrives
        timeout_ms = 500
        waiting_ack = {}
        start = ticks_ms()
        while self.out_q.qsize() > 0 or waiting_ack:
            try:
                out_q_t: OutQueMsg | OutQueAck = await asyncio.wait_for(
                    self.out_q.get(), timeout_ms / 1000
                )
                if type(out_q_t) == OutQueMsg:
                    waiting_ack[wait_index(out_q_t)] = out_q_t
                    await send_message(
                        self.__espnow, out_q_t.mac, out_q_t.msg, sync=False
                    )
                elif type(out_q_t) == OutQueAck:
                    w_index = wait_index(out_q_t)
                    if w_index in waiting_ack:
                        print(f"ack mach {out_q_t=}")
                        del waiting_ack[w_index]

                if ticks_diff(ticks_ms(), start) > timeout_ms:
                    raise asyncio.TimeoutError

            except asyncio.TimeoutError:
                ack_items = waiting_ack.items()
                for k, out_que_msg in ack_items:
                    if out_que_msg.retry <= 0:
                        print(f"retry timeout {k=} {out_que_msg=}")
                        del waiting_ack[k]
                        continue

                    print(f"<<{'r'*out_que_msg.retry}{out_que_msg.msg} {out_que_msg=}")
                    await send_message(
                        self.__espnow, out_que_msg.mac, out_que_msg.msg, sync=False
                    )
                    waiting_ack[k] = OutQueMsg(
                        out_que_msg.msg,
                        out_que_msg.mac,
                        out_que_msg.id,
                        out_que_msg.retry - 1,
                    )

                start = ticks_ms()

        print("sender done")

    @classmethod
    def send_msg(cls, msg: BadgeMsg, mac, sync=False, retry=3):
        out_q = cls.__instance.out_q
        out_q.put_nowait(OutQueMsg(msg.srlz(), mac, msg.id, retry))

        # start sender task
        if cls.__instance._sender_t is None or cls.__instance._sender_t.done():
            cls.__instance._sender_t = asyncio.create_task(cls.__instance._sender())

    @classmethod
    def register_con(cls, connection: "Connection"):
        """
        Registers a new connection and adds the respective peer in ESP-NOW.

        Args:
            connection (Connection): The connection instance to register.
        """
        print(f"register: {connection.con_id}")
        cls.connections[connection.con_id] = connection
        try:
            cls.__espnow.add_peer(connection.c_mac)
        except Exception:
            pass

    @classmethod
    def unregister_con(cls, connection: "Connection"):
        """
        Unregisters a connection and removes it from the active connections.

        Args:
            connection (Connection): The connection instance to unregister.
        """
        if connection.con_id in cls.connections:
            print(f"unregister: {connection.con_id}")
            del cls.connections[connection.con_id]
            # Note: We intentionally do NOT clean up the delivered deque here.
            # Keeping old message IDs prevents stale messages (still in retry queues)
            # from being re-delivered in new sessions. The deque's max size will
            # naturally evict old entries over time.

    @classmethod
    def start(cls, espnow):
        """
        Starts the NowListener instance if not already started.

        Args:
            espnow (aioespnow.AIOESPNow): ESP-NOW instance to handle communication.

        Returns:
            asyncio.Task: The asyncio task running the main task.
        """
        if not cls.__instance:
            cls.__instance = cls(espnow)
            cls.__task = asyncio.create_task(cls.__instance.task())
            cls.__cleanup_task = asyncio.create_task(cls.__instance.cleanup_task())
            return cls.__task

    @classmethod
    def stop(cls):
        """
        Stops the NowListener instance if it is running.
        """
        if cls.__task:
            cls.__task.cancel()
            cls.__task = None

    async def dispatch_app_msg(self, app_msg: AppMsg, s_mac):
        """
        Dispatches an application message to the corresponding connection.

        Args:
            app_msg (AppMsg): The application message.

        Returns:
            bool: True if the message was dispatched, False otherwise.
        """
        if app_msg.con_id in self.connections:
            if self.connections[app_msg.con_id].c_mac != s_mac:
                print(f"con_id mismatch {app_msg.con_id=} {s_mac=}")
                # TODO: send ConTerm for mismached
                return False
            # Pass only the inner content to app
            # filter out retries, don't deliver message with same id
            w_index = wait_index_mac(s_mac, msg_id=app_msg.id)
            if w_index not in NowListener.delivered:
                await self.connections[app_msg.con_id].recv_msg(app_msg.content)
                NowListener.delivered.append(w_index)
                return True
            else:
                print(f"Filtered out {w_index=} {app_msg=}")

        return False

    async def dispatch_msg(self, msg: BadgeMsg, con_id, s_mac):
        """
        Dispatches a message to the corresponding connection based on connection ID.

        Args:
            msg (BadgeMsg): The message to dispatch.
            con_id: The connection ID.

        Returns:
            bool: True if the message was dispatched, False otherwise.
        """
        if con_id in self.connections:
            conn = self.connections[con_id]
            if conn.c_mac != s_mac:
                print(f"con_id mismatch {con_id=} {s_mac=}")
                # TODO: send ConTerm for mismached
                return False

            # Validate session ID for AppMsg to prevent cross-session message routing
            if isinstance(msg, AppMsg):
                msg_session = getattr(msg, 'session_id', None)
                if msg_session is not None and msg_session != conn.session_id:
                    print(f"session_id mismatch: msg={msg_session} conn={conn.session_id}, ignoring stale message")
                    # Mark as delivered even though we're ignoring it, to prevent repeated checks
                    w_index = wait_index_mac(s_mac, msg_id=msg.id)
                    if w_index not in NowListener.delivered:
                        NowListener.delivered.append(w_index)
                    # Still send ACK to prevent retries, but don't deliver the message
                    await send_message(
                        self.__espnow, s_mac, AckMsg(id=msg.id).srlz(), sync=False
                    )
                    return True

            # filter out retries, don't deliver message with same id
            w_index = wait_index_mac(s_mac, msg_id=msg.id)
            if w_index not in NowListener.delivered:
                await conn.recv_msg(msg)
                NowListener.delivered.append(w_index)

            # despite was msg retry or not send ack
            await send_message(
                self.__espnow, s_mac, AckMsg(id=msg.id).srlz(), sync=False
            )
            return True
        return False  # Connection was not found

    @classmethod
    async def conn_req(cls, mac, app_id):
        # Send connection request for app_id to other badge
        # and if then conn is accepted open same app in current badge
        c = Connection(mac, app_id, NowListener.__espnow)
        if await c.connect():  # send connection request
            # change app if request accepted
            await cls.con_cb(c, req=True)
            return True

        return False


class Beacon(AProc):
    # >>> Beacon.setup(espnow, id: BeaconMsg)
    # will setup the Beacon class to use the given espnow instance,
    # Beacon.start(task=True) will return a asyncio.task ans start running Beacon
    # Beacon.stop() will cancel the running task
    # Beacon.suspend(True|False) will suspend/resume the Beacon task # why not to use stop start?
    __espnow: aioespnow.AIOESPNow = None
    __id: BeaconMsg = None
    peer = None
    _susp = asyncio.Event()
    timeout = 5
    _task = None

    @classmethod
    def suspend(cls, value: bool):
        cls._susp.clear() if value else cls._susp.set()

    @classmethod
    async def task(cls, *args, **kwargs):
        try:
            while not cls.stop_event.is_set():
                msg = BeaconMsg(nick=cls.__id.nick).srlz()
                await send_message(cls.__espnow, cls.peer, msg)
                await asyncio.sleep(cls.timeout)
                if not cls._susp.is_set():
                    print("Beacon suspended...")
                    await cls._susp.wait()
                    print("...Beacon resumed")
        except Exception as e:
            print(f"Beacon exeption {e}")

    @classmethod
    def setup(cls, espnow, id: BeaconMsg, peer=b"\xbb\xbb\xbb\xbb\xbb\xbb", timeout=5):
        Beacon.__id = id
        Beacon.__espnow = espnow
        Beacon.timeout = timeout
        Beacon._susp.set()
        Beacon.peer = peer
        try:
            Beacon.__espnow.add_peer(peer)
        except OSError as err:
            if len(err.args) < 2:
                raise err
            if err.args[1] == "ESP_ERR_ESPNOW_EXIST":
                print("Addr exist")
