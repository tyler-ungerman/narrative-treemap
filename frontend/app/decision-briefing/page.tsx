import { redirect } from "next/navigation";

export default function DecisionBriefingRedirect() {
  redirect("/?view=briefing");
}
