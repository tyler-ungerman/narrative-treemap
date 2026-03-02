import { redirect } from "next/navigation";

export default function AlertsRedirect() {
  redirect("/?view=alerts");
}
