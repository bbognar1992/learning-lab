def generate_pw(
        pw_len: int, use_uppercase: bool, use_digits: bool, use_special: bool
):
    import random
    letters = "".join([chr(c) for c in range(97, 123)])
    digits = "".join([str(n) for n in range(0, 10)])
    special = "!@#$%&*^|()_+"

    all_chars = letters
    if use_uppercase:
        all_chars += letters.upper()
    if use_digits:
        all_chars += digits
    if use_special:
        all_chars += special
    result = ""
    for i in range(0, pw_len):
        result += random.choice(all_chars)
    return result


def check_positive_int(s: str, start: int = 1, end: int = 50):
    result = 0
    while result == 0:
        try:
            tmp = int(input(s))
            if start <= tmp <= end:
                result = tmp
            else:
                print(f"Number is not is {start} - {end} range.")
        except Exception:
            print("Provide integer value!")
    return result


def check_yn(s: str):
    result = ""
    while result == "":
        tmp = input(s)
        if tmp == "y":
            result = True
        elif tmp == "n":
            result = False
        else:
            print("Provide n or y value!")
    return result


def pw_generation_page():
    pw_len = check_positive_int("Provide a password length: ", 0, 50)
    pw_use_uppercase = check_yn("Use uppercase letters? (y/n): ")
    pw_use_digits = check_yn("Use digits? (y/n): ")
    pw_use_specials = check_yn("Use special characters? (y/n): ")

    g_password = generate_pw(
        pw_len, pw_use_uppercase, pw_use_digits, pw_use_specials
    )
    print("")
    print("Generating password: ", g_password)
    print("")


if __name__ == "__main__":
    page = 1
    print("-- Password Generator --")
    while page != 2:
        if page == 1:
            print("Choose option:")
            print("1: generate password")
            print("2: exit the program")
            chosen_option = input("Your choice: ")

            if chosen_option == "1":
                page = 3
            elif chosen_option == "2":
                page = 2
            else:
                print("Please enter a correct value.")
        elif page == 2:
            exit()
        elif page == 3:
            pw_generation_page()
            page = 1
        else:
            pass
