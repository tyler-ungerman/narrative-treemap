import { redirect } from "next/navigation";

export default function PaperPortfolioRedirect() {
  redirect("/?view=portfolio");
}
