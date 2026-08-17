import java.util.BitSet;

public class replace {
 
    public static String getNewString(String str, String prefix, String term) {

        // check empty inputs
        if (str == null || str.isEmpty() || term == null || term.isEmpty() || prefix == null || prefix.isEmpty()) {
            return str;
        }

        StringBuilder result = new StringBuilder();
        int i = 0;

        // loop through str and search the term from the advancing index
        // build the output as we go
        while (i < str.length()) {
            
            
            if (str.startsWith(term, i)) {
                result.append(prefix);
                result.append(term);
                i += term.length();
            } else {
                result.append(str.charAt(i));
                i++;
            }
        }

        return result.toString();
    }


    public static int[] calcArray(int[] nums) {
        int n = nums.length;
        int[] result = new int[n];

        // Pass 1: fill result[i] with the product of all elements to the LEFT of i
        // result[0] = 1 because there are no elements to the left of index 0
        result[0] = 1;
        for (int i = 1; i < n; i++) {
            result[i] = result[i - 1] * nums[i - 1];
        }

        // Pass 2: multiply result[i] by the running suffix product (product of all
        // elements to the RIGHT of i), maintained in a single variable
        int suffixProduct = 1;
        for (int i = n - 1; i >= 0; i--) {
            result[i] *= suffixProduct;
            suffixProduct *= nums[i];
        }

        return result;
    }

    public static int[] calcArray2(int[] nums) {
        int n = nums.length;
        int[] result = new int[n];

        for (int i = 0; i < n; i++) {
            int product = 1;
            for (int j = 0; j < n; j++) {
                if (j != i) {
                    product *= nums[j];
                }
            }
            result[i] = product;
        }

        return result;
    }

    public static int getMissingNumber(int[] A) {
        int n = A.length;

        BitSet bitSet = new BitSet(n + 1);
        
        for (int num : A) {
            bitSet.set(num);
        }
        
        return bitSet.nextClearBit(0);
    }

    public static void main(String[] args) {

        System.out.println();
        System.out.println(getNewString("", "_", "alpha"));
        System.out.println();


        int[] res = calcArray(new int[] {2,3,5,1});
        for (int v : res) {
            System.err.print(String.valueOf(v)+" ");
        }
    }
}
