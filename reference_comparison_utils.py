"""Comparison utils for references used as temporary measure before a comparison class is created"""


def compare_arrays(array1, array2):
    """Simple comparison of two arrays of hashed dois, either cited or citing, if it is empty it returns nothing.
    Goes through all the elements and finds how many are equal

    Parameters
    ----------
    array1 : array of spacy objects

    array2 : array of spacy objects

    Returns
    -------
    float
        number
    """
    if array1 != [] and array2 != []:
        if len(array1) < len(array2):
            max_len: int = len(array1)
        else:
            max_len: int = len(array2)
        actual_len: int = 0
        for doi in array1:
            if doi in array2:
                actual_len += 1
        return actual_len / max_len
    else:
        return 0
