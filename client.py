import struct
import socket
import sys


def main(args):
    dest = args[0], int(args[1], 10)
    command = args[2]
    args = args[3:]

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    if command == 'read_object':
        pool, object_id = args
        msg = struct.pack(
            '>LL',
            42,
            len(pool),
        )
        msg += pool.encode('utf-8')
        msg += struct.pack(
            '>BL',
            0x01,
            len(object_id),
        )
        msg += object_id.encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, dest)
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, exist = struct.unpack('>LB', data[0:5])
        assert ctr == 42
        if exist:
            print("Data exist, value: %r" % data[5:])
        else:
            assert len(data) == 5
            print("Data doesn't exist")
    elif command == 'read_part':
        pool, object_id, offset, length = args
        msg = struct.pack(
            '>LL',
            42,
            len(pool),
        )
        msg += pool.encode('utf-8')
        msg += struct.pack(
            '>BL',
            0x02,
            len(object_id),
        )
        msg += object_id.encode('utf-8')
        msg += struct.pack(
            '>LL',
            int(offset, 10),
            int(length, 10),
        )
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, dest)
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, exist = struct.unpack('>LB', data[0:5])
        assert ctr == 42
        if exist:
            print("Data exist, value: %r" % data[5:])
        else:
            assert len(data) == 5
            print("Data doesn't exist")
    elif command == 'write_object':
        pool, object_id, data = args
        msg = struct.pack(
            '>LL',
            42,
            len(pool),
        )
        msg += pool.encode('utf-8')
        msg += struct.pack(
            '>BL',
            0x03,
            len(object_id),
        )
        msg += object_id.encode('utf-8')
        msg += data.encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, dest)
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, = struct.unpack('>L', data)
        assert ctr == 42
    elif command == 'write_part':
        pool, object_id, offset, data = args
        msg = struct.pack(
            '>LL',
            42,
            len(pool),
        )
        msg += pool.encode('utf-8')
        msg += struct.pack(
            '>BL',
            0x04,
            len(object_id),
        )
        msg += object_id.encode('utf-8')
        msg += struct.pack(
            '>L',
            int(offset, 10),
        )
        msg += data.encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, dest)
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, = struct.unpack('>L', data)
        assert ctr == 42
    else:
        raise AssertionError


if __name__ == '__main__':
    main(sys.argv[1:])
