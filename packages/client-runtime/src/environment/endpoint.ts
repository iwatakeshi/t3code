export * from "@t3tools/shared/advertisedEndpoint";

export const environmentEndpointUrl = (httpBaseUrl: string, pathname: string): string => {
  const url = new URL(httpBaseUrl);
  url.pathname = `${environmentBasePath(httpBaseUrl)}${pathname}`;
  url.search = "";
  url.hash = "";
  return url.toString();
};

export const environmentBasePath = (httpBaseUrl: string): string => {
  const path = new URL(httpBaseUrl).pathname;
  return /^\/api\/peer-conversations\/[^/]+\/?$/.test(path) ? path.replace(/\/$/, "") : "";
};
