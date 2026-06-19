import React from "react";
import { Composition } from "remotion";
import { ProjectIntro } from "./video";

export const FPS = 30;
export const WIDTH = 1920;
export const HEIGHT = 1080;
export const DURATION_SECONDS = 126;

export const RemotionRoot: React.FC = () => {
  return (
    <Composition
      id="ProjectIntro"
      component={ProjectIntro}
      durationInFrames={DURATION_SECONDS * FPS}
      fps={FPS}
      height={HEIGHT}
      width={WIDTH}
    />
  );
};
