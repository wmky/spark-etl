public class CharTest {
    public static void main(String args[]){
        char[] charArr = {'a','b','c'};
        char[] charArr2 = charArr;
        charArr2[1]='x';
        System.out.println(charArr2);
        //char数组的打印有点特殊，int数组打印是打印出来一个地址，而char数组是打印数组里的内容。
    }
}
