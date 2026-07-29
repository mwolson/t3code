import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { OpenCode2Icon, OpenCodeIcon } from "./Icons";

describe("OpenCode icons", () => {
  it("keeps the versioned mark aligned and legible at small sizes", () => {
    const openCode = renderToStaticMarkup(<OpenCodeIcon />);
    const openCode2 = renderToStaticMarkup(<OpenCode2Icon />);

    expect(openCode).toContain('viewBox="0 0 32 40"');
    expect(openCode2).toContain('viewBox="0 0 32 40"');
    expect(openCode2).not.toContain("stroke=");
    expect(openCode2).toContain('data-provider-icon="opencode2"');
  });
});
