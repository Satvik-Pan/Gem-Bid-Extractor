import type { Metadata } from "next";
import { IBM_Plex_Sans, JetBrains_Mono } from "next/font/google";
import "./globals.css";

const plexSans = IBM_Plex_Sans({
  variable: "--font-plex-sans",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
});

const jetMono = JetBrains_Mono({
  variable: "--font-jet-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "GEM Bid Operations Dashboard",
  description: "Operational dashboard for extracted and doubtful cybersecurity bids",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${plexSans.variable} ${jetMono.variable}`}>
      <body>{children}</body>
    </html>
  );
}
