import * as Immutable from "immutable";
import * as he from "he";
import urlRegex from "url-regex";
import {
  CellFormatter,
  ClickHandler,
  ClickHandlerAppContext,
  FormatOptions,
} from "./FormatOptions";

/*
 * External link opening is handled through IHostBridge
 * Delegated click blocker prevents default anchor navigation without CSP-breaking inline onclick.
 */
if (typeof window !== "undefined" && typeof document !== "undefined") {
  (window as any).tadLinkNOP = (_url: string) => false;
  if (!(window as any).__pulexLinkGuardInstalled) {
    (window as any).__pulexLinkGuardInstalled = true;
    document.addEventListener(
      "click",
      (evt: MouseEvent) => {
        const target = evt.target as HTMLElement | null;
        if (target && typeof target.closest === "function" && target.closest("a.pulex-cell-link")) {
          evt.preventDefault();
        }
      },
      true
    );
  }
}

export interface TextFormatOptionsProps {
  type: string;
  urlsAsHyperlinks: boolean;
}

const defaultTextFormatOptionsProps: TextFormatOptionsProps = {
  type: "TextFormatOptions",
  urlsAsHyperlinks: true,
};

const isValidURL = (s: string): boolean => {
  if (typeof s !== "string" || s.length > 4096) {
    return false;
  }
  return urlRegex({
    exact: true,
  }).test(s);
};

function stringify(value: any): string | null {
  if (value === null) {
    return null;
  }
  switch (typeof value) {
    case "string":
      return value;
    case "object":
      const ret = JSON.stringify(value, (_, v) =>
        typeof v === "bigint" ? v.toString() : v
      );
      return ret;
    default:
      return String(value);
  }
}

export class TextFormatOptions
  extends Immutable.Record(defaultTextFormatOptionsProps)
  implements TextFormatOptionsProps, FormatOptions {
  public readonly type!: string;
  public readonly urlsAsHyperlinks!: boolean;

  getFormatter(): CellFormatter {
    const ff = (val?: any): string | undefined | null => {
      if (this.urlsAsHyperlinks && typeof val === "string" && val && isValidURL(val)) {
        const safeUrl = he.encode(val);
        return `<a class="pulex-cell-link" rel="noopener noreferrer" href="${safeUrl}">${safeUrl}</a>`;
      }
      const valStr = stringify(val);
      const fmtStr = valStr ? he.encode(valStr) : valStr;
      return fmtStr;
    };

    return ff;
  }

  getClassName(): string | null {
    return null;
  }

  getClickHandler(): ClickHandler {
    const ch = (
      appContext: ClickHandlerAppContext,
      _row: number,
      _column: number,
      val: any
    ) => {
      if (this.urlsAsHyperlinks && typeof val === "string" && val && isValidURL(val)) {
        appContext.openURL(val);
      }
    };
    return ch;
  }
}
