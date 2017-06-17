package iiis.systems.os.blockdb;

public class GetHeightResult {
	public int height;
	public String leafHash;
	
	public GetHeightResult(int height, String leafHash){
		this.height = height;
		this.leafHash = leafHash;
	}
}
