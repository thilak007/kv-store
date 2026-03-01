just p1::test1

just p1::test4

just p1::fuzz 1 "no" "127.0.0.1:3777"
just p1::fuzz 1 "yes" "127.0.0.1:3777"

just p1::fuzz 2 "no" "127.0.0.1:3777"
just p1::fuzz 2 "yes" "127.0.0.1:3777"