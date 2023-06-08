import java.util.Scanner;

public class GenerateRandomNumbers {
	private static double rai=0f;
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter a radious: ");
		rai = sc.nextDouble();
		System.out.println("Enter a total index pair of (x,y): ");
		int index = sc.nextInt();
		
		
		int x[] = new int[index];
		int y[] = new int[index];
		sc.close();
	
		for (int i = 0; i < index; i++) {
		   x[i] = (int) (Math.random() * (rai + 1));
		   y[i] = (int) (Math.random() * (rai + 1));
	   
		   System.out.println("(" +x[i] + "," + y[i] + ")");
			}

	}}
		