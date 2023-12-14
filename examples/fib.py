def slow_fib(n: int) -> int:
    if n <= 1:
        return 1
    return slow_fib(n - 1) + slow_fib(n - 2)
