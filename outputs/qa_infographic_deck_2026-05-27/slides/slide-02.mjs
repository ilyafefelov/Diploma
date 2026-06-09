import { renderQaSlide, SLIDES } from "./common.mjs";

export async function slide02(presentation, ctx) {
  return renderQaSlide(presentation, ctx, SLIDES[1]);
}
