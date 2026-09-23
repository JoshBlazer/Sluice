"use client";
import Link from "next/link";
import { usePathname } from "next/navigation";

const LINKS = [
  { href: "/", label: "Queue" },
  { href: "/runs", label: "Runs" },
  { href: "/retries", label: "Retries" },
  { href: "/dead-letter", label: "Dead Letter" },
];

export function NavBar() {
  const pathname = usePathname();
  const isActive = (href: string) =>
    href === "/" ? pathname === "/" : pathname.startsWith(href);

  return (
    <nav className="border-b border-zinc-800 bg-zinc-950/80 backdrop-blur-sm sticky top-0 z-10 px-6 flex items-stretch gap-1 text-sm">
      <span className="font-bold text-white mr-5 py-3.5 tracking-tight flex items-center gap-1.5 text-base">
        <span className="text-orange-400">⚡</span>
        <span>Sluice</span>
      </span>
      {LINKS.map(({ href, label }) => (
        <Link
          key={href}
          href={href}
          className={`py-3.5 px-3 border-b-2 transition-all text-sm font-medium ${
            isActive(href)
              ? "border-blue-500 text-white"
              : "border-transparent text-zinc-500 hover:text-zinc-200 hover:border-zinc-600"
          }`}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}
