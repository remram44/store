import struct
import socket
import sys


def main(args):
    if args[0] == 'read_object':
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = struct.pack(
            '>LBL',
            42,
            0x01,
            len(args[3]),
        )
        msg += args[3].encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, (args[1], int(args[2], 10)))
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, exist = struct.unpack('>LB', data[0:5])
        assert ctr == 42
        if exist:
            print("Data exist, value: %r" % data[5:])
        else:
            assert len(data) == 5
            print("Data doesn't exist")
    elif args[0] == 'read_part':
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = struct.pack(
            '>LBL',
            42,
            0x02,
            len(args[3]),
        )
        msg += args[3].encode('utf-8')
        msg += struct.pack(
            '>LL',
            int(args[4], 10),
            int(args[5], 10),
        )
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, (args[1], int(args[2], 10)))
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, exist = struct.unpack('>LB', data[0:5])
        assert ctr == 42
        if exist:
            print("Data exist, value: %r" % data[5:])
        else:
            assert len(data) == 5
            print("Data doesn't exist")
    elif args[0] == 'write_object':
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = struct.pack(
            '>LBL',
            42,
            0x03,
            len(args[3]),
        )
        msg += args[3].encode('utf-8')
        msg += args[4].encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, (args[1], int(args[2], 10)))
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, = struct.unpack('>L', data)
        assert ctr == 42
    elif args[0] == 'write_part':
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = struct.pack(
            '>LBL',
            42,
            0x04,
            len(args[3]),
        )
        msg += args[3].encode('utf-8')
        msg += struct.pack(
            '>L',
            int(args[4], 10),
        )
        msg += args[5].encode('utf-8')
        print("Sending packet: %s size %d" % (
            ''.join('%02x,' % b for b in msg),
            len(msg)
        ))
        sock.sendto(msg, (args[1], int(args[2], 10)))
        data, addr = sock.recvfrom(65536)
        print("Got response, size %d" % len(data))
        ctr, = struct.unpack('>L', data)
        assert ctr == 42
    else:
        raise AssertionError


if __name__ == '__main__':
    main(sys.argv[1:])
