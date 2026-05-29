import { renderQaSlide, SLIDES } from "./common.mjs";

export async function slide01(presentation, ctx) {
  return renderQaSlide(presentation, ctx, SLIDES[0]);
}
