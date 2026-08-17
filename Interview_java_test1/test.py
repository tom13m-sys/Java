def get_new_string(str_val: str, prefix: str, term: str) -> str:
    # check empty inputs
    if not str_val or not term or not prefix:
        return str_val

    result = []
    i = 0
    
    # loop through str and search the term from the advancing index
    # build the output as we go
    while i < len(str_val):
        if str_val.startswith(term, i):
            result.append(prefix)
            result.append(term)
            i += len(term)
        else:
            result.append(str_val[i])
            i += 1
            
    return "".join(result)

def product_of_without_self(nums: list[int]) -> list[int]:
    n = len(nums)
    
    # init an array full with 1
    result = [1] * n

    # Pass 1: fill result[i] with the product of all elements to the LEFT of i
    # result[0] = 1 because there are no elements to the left of index 0
    for i in range(1, n):
        result[i] = result[i - 1] * nums[i - 1]

    # Pass 2: multiply result[i] by the running suffix product (product of all elements to the RIGHT of i)
    suffix_product = 1
    for i in range(n - 1, -1, -1):
        result[i] *= suffix_product
        suffix_product *= nums[i]

    return result


def get_missing_number(nums: list[int]):

    SUM = 45
    actual_sum = 0
    for num in nums:
        actual_sum += num
    
    return SUM - actual_sum




if __name__ == "__main__":
    
    #print(product_except_self([2,3,5,1]))
    print(get_missing_number([6,2,3,5,1,7,9,4,8]))


