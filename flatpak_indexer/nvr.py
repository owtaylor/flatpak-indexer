from version_utils.rpm import compare_versions


class NVR(str):
    def __new__(cls, val):
        if isinstance(val, NVR):
            return val

        result = super().__new__(cls, val)
        if result.count("-") < 2:
            raise ValueError("Argument to NVR() must have at least two dashes")

        return result

    @property
    def name(self):
        return self.rsplit("-", 2)[0]
        # Slightly slower for CPython <= 3.11
        # dash = self.rfind("-", 0, self.rfind("-"))
        # return self[0:dash]

    @property
    def version(self):
        return self.rsplit("-", 2)[1]
        # Slightly slower for CPython <= 3.11
        # dash2 = self.rfind("-")
        # dash1 = self.rfind("-", 0, dash2)
        # return self[dash1 + 1:dash2]

    @property
    def release(self):
        return self[self.rfind("-") + 1 :]

    def __lt__(self, other):
        n_a, v_a, r_a = self.rsplit("-", 2)
        n_b, v_b, r_b = other.rsplit("-", 2)

        if n_a == n_b:
            version_cmp = compare_versions(v_a, v_b)
            if version_cmp == 0:
                return compare_versions(r_a, r_b) < 0
            else:
                return version_cmp < 0
        else:
            return n_a < n_b

    def __le__(self, other):
        return self == other or self < other

    def __gt__(self, other):
        return self != other and not self < other

    def __ge__(self, other):
        return not self < other

    def __repr__(self):
        return f"NVR({super().__repr__()})"
