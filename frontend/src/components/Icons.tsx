import type { SVGProps } from 'react';
export function Icon({
  name,
  ...props
}: SVGProps<SVGSVGElement> & {
  name:
    | 'send'
    | 'plus'
    | 'search'
    | 'sun'
    | 'moon'
    | 'logout'
    | 'back'
    | 'close'
    | 'chat';
}) {
  const paths = {
    send: 'm4 4 17 8-17 8 3-8-3-8Zm3 8h14',
    plus: 'M12 5v14M5 12h14',
    search: 'm16 16 5 5M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0',
    sun: 'M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0',
    moon: 'M20 15.5A8.5 8.5 0 0 1 8.5 4 8.5 8.5 0 1 0 20 15.5',
    logout: 'M10 4H4v16h6m5-12 4 4-4 4m-7-4h11',
    back: 'm14 6-6 6 6 6',
    close: 'm6 6 12 12M6 18 18 6',
    chat: 'M20 11a8 8 0 0 1-8 8H5l-3 3V11a9 9 0 0 1 18 0Z',
  };
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.65"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      <path d={paths[name]} />
    </svg>
  );
}
export function Brand({ compact = false }: { compact?: boolean }) {
  return (
    <div className="brand">
      <span className="brand-mark" aria-hidden="true">
        <svg viewBox="0 0 32 32" fill="none">
          <path
            d="M5 17h6l3-9 5 17 3-8h5"
            stroke="currentColor"
            strokeWidth="2.2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </span>
      {!compact && (
        <span>
          PulseChat<span className="brand-period">.</span>
        </span>
      )}
    </div>
  );
}
export function Avatar({
  name,
  small = false,
}: {
  name: string;
  small?: boolean;
}) {
  return (
    <span className={`avatar ${small ? 'small' : ''}`} aria-hidden="true">
      {name.slice(0, 2).toUpperCase()}
    </span>
  );
}
