import { renderQaSlide, SLIDES } from "./common.mjs";

export async function slide05(presentation, ctx) {
  return renderQaSlide(presentation, ctx, SLIDES[4]);
}
