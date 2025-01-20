import math
import time
import sys

def forbidden_method():
    A = 0.0
    B = 0.0
    width = 80
    height = 22
    size = width * height
    b = [' ' for _ in range(size)]
    z = [0.0 for _ in range(size)]
    chars = ".,-~:;=!*#$@"

    sys.stdout.write("\x1b[2J")
    sys.stdout.flush()

    try:
        while True:
            for i in range(size):
                b[i] = ' '
                z[i] = 0.0

            j = 0.0
            while j < 6.28:
                i = 0.0
                while i < 6.28:
                    c = math.sin(i)
                    d = math.cos(j)
                    e = math.sin(A)
                    f = math.sin(j)
                    g = math.cos(A)
                    h = d + 2.0
                    D = 1.0 / (c * h * e + f * g + 5.0)
                    l = math.cos(i)
                    m = math.cos(B)
                    n = math.sin(B)
                    t = c * h * g - f * e

                    x = int(40 + 30 * D * (l * h * m - t * n))
                    y = int(12 + 15 * D * (l * h * n + t * m))
                    o = x + width * y

                    N = int(8 * ((f * e - c * d * g) * m - c * d * e - f * g - l * d * n))
                    if N < 0:
                        N = 0
                    elif N >= len(chars):
                        N = len(chars) - 1

                    if 0 < y < height and 0 < x < width and D > z[o]:
                        z[o] = D
                        b[o] = chars[N]

                    i += 0.02
                j += 0.07

            sys.stdout.write("\x1b[H")

            output = []
            for k in range(size):
                if k % width == 0 and k != 0:
                    output.append('\n')
                output.append(b[k])
            sys.stdout.write(''.join(output))
            sys.stdout.flush()

            A += 0.0704
            B += 0.0352

            time.sleep(0.03)
    except KeyboardInterrupt:
        sys.exit()