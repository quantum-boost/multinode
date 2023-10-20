from typing import Dict, List


class YieldFnFinal:
    strings_so_far: List[Dict[int, str]] = []

    def __init__(self, strings_so_far: List[Dict[int, str]]):
        self.strings_so_far = strings_so_far

    @property
    def first_string(self):
        return self.strings_so_far[0]
