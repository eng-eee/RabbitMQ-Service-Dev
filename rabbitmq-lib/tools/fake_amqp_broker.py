import socket, struct, sys, threading, time

def frame(t, ch, payload): return struct.pack('>BHI', t, ch, len(payload)) + payload + b'\xce'
def method(cls, mth, args=b''): return frame(1, 0, struct.pack('>HH', cls, mth) + args)
def longstr(b): return struct.pack('>I', len(b)) + b

def serve(conn, hb, send_hb):
    conn.recv(8)
    conn.sendall(method(10, 10, bytes([0, 9]) + struct.pack('>I', 0) + longstr(b'PLAIN') + longstr(b'en_US')))
    stop = threading.Event()
    def beat():
        while not stop.wait(max(hb / 2, 0.5)):
            try: conn.sendall(frame(8, 0, b''))
            except OSError: return
    if send_hb: threading.Thread(target=beat, daemon=True).start()
    buf = b''
    try:
        while True:
            d = conn.recv(4096)
            if not d: return
            buf += d
            while len(buf) >= 7:
                t, ch, size = struct.unpack('>BHI', buf[:7])
                if len(buf) < 8 + size: break
                payload, buf = buf[7:7 + size], buf[8 + size:]
                if t == 8: continue
                cls, mth = struct.unpack('>HH', payload[:4])
                if (cls, mth) == (10, 11): conn.sendall(method(10, 30, struct.pack('>HIH', 0, 131072, hb)))
                elif (cls, mth) == (10, 40): conn.sendall(method(10, 41, b'\x00'))
                elif (cls, mth) == (10, 50): conn.sendall(method(10, 51)); return
    finally:
        stop.set(); conn.close()

port, hb, send_hb, life = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3] == '1', float(sys.argv[4])
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', port)); s.listen(5); s.settimeout(0.2)
end = time.time() + life
while time.time() < end:
    try: c, _ = s.accept()
    except socket.timeout: continue
    threading.Thread(target=serve, args=(c, hb, send_hb), daemon=True).start()
