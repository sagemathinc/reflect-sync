import { parseEndpoint } from "../session-manage.js";

describe("parseEndpoint", () => {
  it("returns local path details for absolute paths", () => {
    expect(parseEndpoint("/tmp/example")).toEqual({ root: "/tmp/example" });
  });

  it("parses remote host with explicit port", () => {
    expect(parseEndpoint("user@example.com:2222:/srv/data")).toEqual({
      host: "user@example.com",
      port: 2222,
      root: "/srv/data",
    });
  });

  it("defaults to port 22 when using double colon", () => {
    expect(parseEndpoint("example.org::/srv/app")).toEqual({
      host: "example.org",
      port: 22,
      root: "/srv/app",
    });
  });

  it("treats remote paths without explicit port as default ssh", () => {
    expect(parseEndpoint("example.org:/srv/app")).toEqual({
      host: "example.org",
      root: "/srv/app",
    });
  });

  it("handles short host aliases without a port", () => {
    expect(parseEndpoint("s:/tmp/b")).toEqual({
      host: "s",
      port: undefined,
      root: "/tmp/b",
    });
  });

  it("requires remote paths to begin with '/' or '~/'", () => {
    expect(() => parseEndpoint("example.org:2222:relative/path")).toThrow(
      "Remote paths must start with '/' or '~/'",
    );
  });

  it("treats Windows drive letter paths as local", () => {
    const windowsPath = String.raw`C:\data`;
    expect(parseEndpoint(windowsPath)).toEqual({ root: windowsPath });
  });
});
