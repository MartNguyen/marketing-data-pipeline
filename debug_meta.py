import os
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError

def debug_account_access():
    token = os.environ.get("FB_ACCESS_TOKEN")
    FacebookAdsApi.init(access_token=token)
    
    target_acc = "act_874972305237436"
    print(f"⏳ Testing access for account: {target_acc}")
    
    try:
        acc = AdAccount(target_acc)
        insights = acc.get_insights(
            fields=["account_id", "spend"],
            params={'time_range': {'since': '2026-04-01', 'until': '2026-04-01'}, 'level': 'account'}
        )
        data = list(insights)
        print(f"✅ Thanh cong! Data tra ve: {data}")
    except FacebookRequestError as e:
        print(f"❌ LOI CHI MANG TU META API: {e.api_error_message()} (Code: {e.api_error_code()})")
    except Exception as e:
        print(f"❌ Loi he thong: {str(e)}")

if __name__ == "__main__":
    debug_account_access()
