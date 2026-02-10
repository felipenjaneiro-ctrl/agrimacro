"use client";
import dynamic from "next/dynamic";
import ChatPanel from "./ChatPanel";
const Dashboard = dynamic(() => import("./dashboard"), { ssr: false });
export default function Home() {
  return <>
    <Dashboard />
    <ChatPanel />
  </>;
}
