import { productionLicenseEntries } from "./license-utils.mjs";

function spdxId(value) {
  return `SPDXRef-${value.replace(/[^A-Za-z0-9.-]+/gu, "-")}`;
}

function npmPurl(name, version) {
  const encodedName = name.startsWith("@")
    ? `%40${name.slice(1).replace("/", "/")}`
    : name;
  return `pkg:npm/${encodedName}@${version}`;
}

function packageEntry({
  id,
  name,
  version,
  license,
  downloadLocation,
  purl,
  homepage,
}) {
  const result = {
    SPDXID: id,
    name,
    versionInfo: version,
    downloadLocation,
    filesAnalyzed: false,
    licenseConcluded: license,
    licenseDeclared: license,
    copyrightText: "NOASSERTION",
  };
  if (homepage) result.homepage = homepage;
  if (purl) {
    result.externalRefs = [
      {
        referenceCategory: "PACKAGE-MANAGER",
        referenceType: "purl",
        referenceLocator: purl,
      },
    ];
  }
  return result;
}

export function createReleaseSpdx({
  root,
  packageMetadata,
  runtimeConfig,
  nodeVersion,
  commit,
  created,
  artifacts,
}) {
  const reflectId = spdxId(`Package-${packageMetadata.name}`);
  const nodeId = spdxId(`Package-node-${nodeVersion}`);
  const rsyncId = spdxId(`Package-rsync-${runtimeConfig.upstreamVersion}`);
  const packages = [
    packageEntry({
      id: reflectId,
      name: packageMetadata.name,
      version: packageMetadata.version,
      license: packageMetadata.license,
      downloadLocation: `https://github.com/sagemathinc/reflect-sync/tree/${commit}`,
      purl: `pkg:npm/${packageMetadata.name}@${packageMetadata.version}`,
      homepage: packageMetadata.homepage,
    }),
    packageEntry({
      id: nodeId,
      name: "node",
      version: nodeVersion,
      license: "MIT",
      downloadLocation: `https://nodejs.org/dist/v${nodeVersion}/`,
      purl: `pkg:generic/node@${nodeVersion}`,
      homepage: "https://nodejs.org/",
    }),
    packageEntry({
      id: rsyncId,
      name: "rsync",
      version: runtimeConfig.upstreamVersion,
      license: "GPL-3.0-or-later",
      downloadLocation: runtimeConfig.sourceUrl,
      purl: `pkg:generic/rsync@${runtimeConfig.upstreamVersion}`,
      homepage: "https://rsync.samba.org/",
    }),
  ];
  const relationships = [
    {
      spdxElementId: "SPDXRef-DOCUMENT",
      relationshipType: "DESCRIBES",
      relatedSpdxElement: reflectId,
    },
    {
      spdxElementId: "SPDXRef-DOCUMENT",
      relationshipType: "DESCRIBES",
      relatedSpdxElement: rsyncId,
    },
    {
      spdxElementId: reflectId,
      relationshipType: "DEPENDS_ON",
      relatedSpdxElement: nodeId,
    },
    {
      spdxElementId: reflectId,
      relationshipType: "DEPENDS_ON",
      relatedSpdxElement: rsyncId,
    },
  ];

  for (const dependency of productionLicenseEntries(root)) {
    for (const version of dependency.versions) {
      const id = spdxId(`Package-npm-${dependency.name}-${version}`);
      packages.push(
        packageEntry({
          id,
          name: dependency.name,
          version,
          license: dependency.license || "NOASSERTION",
          downloadLocation: `https://registry.npmjs.org/${encodeURIComponent(dependency.name)}/-/${dependency.name.split("/").at(-1)}-${version}.tgz`,
          purl: npmPurl(dependency.name, version),
          homepage: dependency.homepage,
        }),
      );
      relationships.push({
        spdxElementId: reflectId,
        relationshipType: "DEPENDS_ON",
        relatedSpdxElement: id,
      });
    }
  }

  const files = artifacts.map((artifact) => {
    const id = spdxId(`File-${artifact.name}`);
    relationships.push({
      spdxElementId: ["rsync-runtime", "rsync-source"].includes(artifact.kind)
        ? rsyncId
        : reflectId,
      relationshipType: "CONTAINS",
      relatedSpdxElement: id,
    });
    return {
      SPDXID: id,
      fileName: `./${artifact.name}`,
      checksums: [{ algorithm: "SHA256", checksumValue: artifact.sha256 }],
      licenseConcluded: "NOASSERTION",
      copyrightText: "NOASSERTION",
    };
  });

  return {
    spdxVersion: "SPDX-2.3",
    dataLicense: "CC0-1.0",
    SPDXID: "SPDXRef-DOCUMENT",
    name: `reflect-sync-${packageMetadata.version}-release`,
    documentNamespace: `https://reflect-sync.dev/.well-known/spdx/reflect-sync-${packageMetadata.version}-${commit}`,
    creationInfo: {
      created,
      creators: ["Tool: reflect-sync-release-assembler"],
    },
    documentDescribes: [reflectId, rsyncId],
    packages,
    files,
    relationships,
  };
}
