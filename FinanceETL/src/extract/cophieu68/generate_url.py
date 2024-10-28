BASE_URL = "https://www.cophieu68.vn/"
SUMMARY ="quote/"
PROFILE = "profile.php?id="


def generate_summary_url(symbol):
    return f"{BASE_URL}{SUMMARY}{PROFILE}{symbol}"