package shared;
public class APIVersions {
  private short apiKey;
  private short minSupportedVersion;
  private short maxSupportedVersion;
  private TagBuffer tagBuffer;
  public APIVersions() {}
  public APIVersions(short apiKey, short minSupportedVersion,
                     short maxSupportedVersion, TagBuffer tagBuffer) {
    this.apiKey = apiKey;
    this.minSupportedVersion = minSupportedVersion;
    this.maxSupportedVersion = maxSupportedVersion;
    this.tagBuffer = tagBuffer;
  }
  public short getApiKey() { return apiKey; }
  public short getMinSupportedVersion() { return minSupportedVersion; }
  public short getMaxSupportedVersion() { return maxSupportedVersion; }
  public TagBuffer getTagBuffer() { return tagBuffer; }
}